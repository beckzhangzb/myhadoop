import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池测试
 * @author zhangzb
 * @since 2019/2/27 17:35
 */
public class ThreadPoolTest {

    public static void main(String[] args) {
        ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("demo-pool-%d").build();

        ExecutorService pool = new ThreadPoolExecutor(5, 20, 6L, TimeUnit.MINUTES,
                new LinkedBlockingDeque<Runnable>(40), tf, new ThreadPoolExecutor.AbortPolicy());

        for (int i = 0; i < 70; i++) {
            pool.execute(new MyTask());
        }
        pool.shutdown();
    }
}

class MyTask implements Runnable {

    @Override
    public void run() {
        try {
            Thread.sleep(5000);
            System.out.println(Thread.currentThread().getName() + "#" + Thread.currentThread().getClass());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
