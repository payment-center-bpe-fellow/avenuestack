package avenuestack.impl.util;

import java.util.concurrent.locks.ReentrantLock;


public class RequestIdGenerator {

	static long savedTime = 0L;
	static int savedIndex = 10000;
	static ReentrantLock lock = new ReentrantLock(false);
	static String serverId = IpUtils.serverId();

    static public String nextId() {

        long now = 0L;
        int index = 0;

        lock.lock();

        try {
            now = System.currentTimeMillis();
            if (now == savedTime) {
                savedIndex+=1;
            } else {
                savedTime = now;
                savedIndex = 10000;
            }

            index = savedIndex;
            } finally {
                lock.unlock();
            }

            return "" + serverId + now + index;
    }
}


