package se.lars.rx;

import io.reactivex.Flowable;
import io.reactivex.functions.Function;

import java.util.concurrent.TimeUnit;

public class RetryWithDelay implements Function<Flowable<? extends Throwable>, Flowable<?>> {

  private final int maxRetryCount;
  private final int retryDelay;
  private final TimeUnit timeUnit;
  private int retryCount;

  public RetryWithDelay(final int maxRetryCount, final int retryDelay, final TimeUnit timeUnit) {
    this.maxRetryCount = maxRetryCount;
    this.retryDelay = retryDelay;
    this.timeUnit = timeUnit;
    this.retryCount = 0;
  }

  @Override
  public Flowable<?> apply(final Flowable<? extends Throwable> attempts) {
    return attempts.flatMap((Function<Throwable, Flowable<?>>) throwable -> {
      if (++retryCount < maxRetryCount) {
        return Flowable.timer(retryDelay, timeUnit);
      }
      return Flowable.error(throwable);
    });
  }
}
