package dbms.client;

import java.io.IOException;

/**
 * Специальный подкласс IOException, отличающий явный отказ сервера (UNAVAILABLE) от обычного
 * обрыва соединения. Это важно для верификации в LocalReplica:
 *
 *  - UNAVAILABLE на PUT означает: запрос точно НЕ применился (сервер ответил, но отклонил).
 *  - Обычный IOException на PUT означает: неизвестно, применился запрос или нет (мог упасть
 *    после fsync, но до отправки ответа). Такой ключ помечается как unknown.
 */
public class UnavailableException extends IOException {

    private static final long serialVersionUID = 1L;

    public UnavailableException(String message) {
        super(message);
    }
}
