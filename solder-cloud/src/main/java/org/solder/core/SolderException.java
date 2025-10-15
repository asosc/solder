package org.solder.core;

import java.io.IOException;
import java.util.Objects;

import org.apache.commons.io.function.IORunnable;

import com.ee.util.exception.EException;
import com.ee.util.exception.ERuntimeException;

public class SolderException  extends IOException {
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 7150779504601688283L;

	/**
	 * 
	 */

	//More convenient..
	public static SolderException rethrow(Throwable t) throws EException {
		throw new EException(t);
	}
	
	public static ERuntimeException rethrowUnchecked(Throwable t) throws ERuntimeException {
		throw new ERuntimeException(t);
	}
	
	public static Runnable unchecked(IORunnable r) {
		Objects.requireNonNull(r,"Runnable");
		return ()-> {
			try {
				r.run();
			}catch(IOException e) {
				rethrowUnchecked(e);
			}
		};
	}
	
	public static void runUnchecked(IORunnable r) {
		Objects.requireNonNull(r,"Runnable");
		try {
			r.run();
		}catch(IOException e) {
			rethrowUnchecked(e);
		}
	}
	
	 /**
     * Constructs an {@code IOException} with {@code null}
     * as its error detail message.
     */
    public SolderException() {
        super();
    }

    /**
     * Constructs an {@code IOException} with the specified detail message.
     *
     * @param message
     *        The detail message (which is saved for later retrieval
     *        by the {@link #getMessage()} method)
     */
    public SolderException(String message) {
        super(message);
    }

    /**
     * Constructs an {@code IOException} with the specified detail message
     * and cause.
     *
     * <p> Note that the detail message associated with {@code cause} is
     * <i>not</i> automatically incorporated into this exception's detail
     * message.
     *
     * @param message
     *        The detail message (which is saved for later retrieval
     *        by the {@link #getMessage()} method)
     *
     * @param cause
     *        The cause (which is saved for later retrieval by the
     *        {@link #getCause()} method).  (A null value is permitted,
     *        and indicates that the cause is nonexistent or unknown.)
     *
     * @since 1.6
     */
    public SolderException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an {@code IOException} with the specified cause and a
     * detail message of {@code (cause==null ? null : cause.toString())}
     * (which typically contains the class and detail message of {@code cause}).
     * This constructor is useful for IO exceptions that are little more
     * than wrappers for other throwables.
     *
     * @param cause
     *        The cause (which is saved for later retrieval by the
     *        {@link #getCause()} method).  (A null value is permitted,
     *        and indicates that the cause is nonexistent or unknown.)
     *
     * @since 1.6
     */
    public SolderException(Throwable cause) {
        super(cause);
    }

}
