package dbms.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * WAL только-на-дозапись, разрезан на сегментные файлы фиксированного размера, лежащие в одной
 * директории.
 *
 * Имя сегмента: {@code wal-<20-значный-startLsn-с-нулями>.log}. В имени файла зашит LSN первой
 * записи в сегменте. Поскольку LSN-ы непрерывны и без дыр, последний LSN запечатанного сегмента
 * однозначно вычисляется как {@code nextSegment.startLsn − 1}.
 *
 * Конкурентность: {@code append} / {@code fsync} / {@code rotate} / {@code deleteSealedThrough}
 * — synchronized; рассчитаны на вызов из единственного commit-потока.
 */
public final class Wal implements AutoCloseable {

    static final String SEGMENT_PREFIX = "wal-";
    static final String SEGMENT_SUFFIX = ".log";
    static final int FILENAME_DIGITS = 20; // вмещает максимальную длину Long.toUnsignedString

    private final Path dir;
    private final long maxSegmentBytes;

    private FileChannel active;
    private Path activePath;
    private long activeStartLsn;
    private long activeWritten;
    private long lastLsn;       // lsn последней записи, дописанной этим writer-ом
    private long nextLsn;       // lsn, который должен использовать следующий append

    private final ByteBuffer scratch =
            ByteBuffer.allocate(WalRecord.SERIALIZED_SIZE).order(ByteOrder.LITTLE_ENDIAN);

    private Wal(Path dir, long maxSegmentBytes) {
        this.dir = dir;
        this.maxSegmentBytes = maxSegmentBytes;
    }

    /**
     * Открыть WAL для дозаписи на указанном next-LSN. Если уже существует сегмент, в который
     * должен попасть этот LSN (то есть последний сегмент не полон и его tail+1 совпадает с
     * указанным LSN) — продолжаем писать в него; иначе катим новый сегмент с именем по {@code
     * nextLsn}.
     *
     * Усечение торн-тейла должно быть выполнено recovery до вызова этого метода.
     */
    public static Wal open(Path dir, long nextLsn, long maxSegmentBytes) throws IOException {
        Files.createDirectories(dir);
        Wal w = new Wal(dir, maxSegmentBytes);
        w.openForAppend(nextLsn);
        return w;
    }

    private void openForAppend(long nextLsnArg) throws IOException {
        this.nextLsn = nextLsnArg;
        this.lastLsn = nextLsnArg - 1;

        List<Path> segs = listSegmentsSorted(dir);
        Path tail = segs.isEmpty() ? null : segs.get(segs.size() - 1);
        if (tail != null) {
            long tailStart = startLsnOf(tail);
            long tailSize = Files.size(tail);
            long tailRecords = tailSize / WalRecord.SERIALIZED_SIZE;
            long tailEndExclusive = tailStart + tailRecords; 
            if (tailSize % WalRecord.SERIALIZED_SIZE != 0) {
                throw new IOException("сегмент wal " + tail
                        + " имеет невыровненный размер " + tailSize
                        + " — recovery должен был усечь его до открытия");
            }
            if (tailEndExclusive == nextLsnArg && tailSize < maxSegmentBytes) {
                activePath = tail;
                active = FileChannel.open(tail,
                        StandardOpenOption.WRITE, StandardOpenOption.READ);
                active.position(tailSize);
                activeStartLsn = tailStart;
                activeWritten = tailSize;
                return;
            }
            if (tailEndExclusive != nextLsnArg) {
                throw new IOException("хвост wal заканчивается lsn " + tailEndExclusive
                        + ", а next-lsn = " + nextLsnArg
                        + " — recovery должен был это согласовать");
            }
        }
        rollNewSegment(nextLsnArg);
    }

    private void rollNewSegment(long startLsn) throws IOException {
        Path p = segmentPath(dir, startLsn);
        FileChannel ch = FileChannel.open(p,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
        ch.position(0);
        this.active = ch;
        this.activePath = p;
        this.activeStartLsn = startLsn;
        this.activeWritten = 0;
        fsyncDir();
    }

    private void fsyncDir() throws IOException {
        try (FileChannel d = FileChannel.open(dir, StandardOpenOption.READ)) {
            d.force(true);
        }
    }

    /** Дописать запись. Caller обязан передавать строго возрастающие LSN, начиная с next-lsn. */
    public synchronized void append(WalRecord r) throws IOException {
        if (r.lsn() != nextLsn) {
            throw new IllegalStateException("ожидался lsn " + nextLsn + ", получен " + r.lsn());
        }
        if (activeWritten + WalRecord.SERIALIZED_SIZE > maxSegmentBytes) {
            active.force(false);
            active.close();
            rollNewSegment(r.lsn());
        }
        scratch.clear();
        r.writeTo(scratch);
        scratch.flip();
        while (scratch.hasRemaining()) {
            active.write(scratch);
        }
        activeWritten += WalRecord.SERIALIZED_SIZE;
        lastLsn = r.lsn();
        nextLsn = r.lsn() + 1;
    }

    /** Сделать дописанные записи durable. {@code metaData=false} → только данные, быстрее. */
    public synchronized void fsync() throws IOException {
        active.force(false);
    }

    /** Сумма байт по всем сегментам WAL (запечатанным и активному). */
    public synchronized long totalBytes() throws IOException {
        long sum = 0;
        for (Path p : listSegmentsSorted(dir)) {
            sum += Files.size(p);
        }
        return sum;
    }

    /**
     * Удалить все запечатанные (не активные) сегменты, у которых весь диапазон LSN
     * {@code <= throughLsn}. Активный сегмент этим вызовом никогда не удаляется.
     */
    public synchronized void deleteSealedThrough(long throughLsn) throws IOException {
        List<Path> segs = listSegmentsSorted(dir);
        for (int i = 0; i < segs.size(); i++) {
            Path p = segs.get(i);
            if (p.equals(activePath)) {
                continue;
            }
            Path next = (i + 1 < segs.size()) ? segs.get(i + 1) : activePath;
            long endLsn = startLsnOf(next) - 1;
            if (endLsn <= throughLsn) {
                Files.deleteIfExists(p);
            }
        }
    }

    public long lastLsn() {
        return lastLsn;
    }

    public long nextLsn() {
        return nextLsn;
    }

    public Path activeSegmentPath() {
        return activePath;
    }

    @Override
    public synchronized void close() throws IOException {
        if (active != null) {
            active.close();
            active = null;
        }
    }


    static Path segmentPath(Path dir, long startLsn) {
        return dir.resolve(SEGMENT_PREFIX
                + String.format("%0" + FILENAME_DIGITS + "d", startLsn) + SEGMENT_SUFFIX);
    }

    static boolean isSegment(Path p) {
        String n = p.getFileName().toString();
        return n.startsWith(SEGMENT_PREFIX) && n.endsWith(SEGMENT_SUFFIX)
                && n.length() == SEGMENT_PREFIX.length() + FILENAME_DIGITS + SEGMENT_SUFFIX.length();
    }

    static long startLsnOf(Path p) {
        String n = p.getFileName().toString();
        String num = n.substring(SEGMENT_PREFIX.length(), n.length() - SEGMENT_SUFFIX.length());
        return Long.parseLong(num);
    }

    static List<Path> listSegmentsSorted(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return List.of();
        }
        List<Path> out = new ArrayList<>();
        try (Stream<Path> s = Files.list(dir)) {
            s.filter(Wal::isSegment).forEach(out::add);
        }
        out.sort(Comparator.comparingLong(p -> {
            try {
                return startLsnOf(p);
            } catch (NumberFormatException e) {
                throw new UncheckedIOException(new IOException("плохое имя сегмента " + p, e));
            }
        }));
        return out;
    }
}
