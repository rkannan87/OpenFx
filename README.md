import java.awt.AWTException;
import java.awt.MouseInfo;
import java.awt.Point;
import java.awt.Robot;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * SystemActivityKeeper
 *
 * Keeps the system (and any active browser window) from going idle by
 * nudging the mouse cursor by a tiny amount at a fixed interval.
 * This resets OS idle timers, which typically prevents:
 *   - screen lock / screensaver activation
 *   - "Away" status in apps like Teams
 *   - browser tab throttling tied to window focus/activity
 *
 * Run directly in IntelliJ: right-click -> Run 'SystemActivityKeeper.main()'
 */
public class SystemActivityKeeper {

    // Interval between nudges, in minutes
    private static final long INTERVAL_MINUTES = 5;

    // How far (in pixels) to move the mouse and back. Small enough to be unnoticeable.
    private static final int NUDGE_PIXELS = 1;

    private static final DateTimeFormatter TIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        Robot robot;
        try {
            robot = new Robot();
        } catch (AWTException e) {
            System.err.println("Unable to initialize Robot (check display/permissions): " + e.getMessage());
            return;
        }

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "activity-keeper-thread");
            t.setDaemon(true); // won't prevent JVM shutdown
            return t;
        });

        System.out.println("SystemActivityKeeper started. Nudging mouse every "
                + INTERVAL_MINUTES + " minute(s). Press Ctrl+C to stop.");

        Runnable nudgeTask = () -> {
            try {
                Point current = MouseInfo.getPointerInfo().getLocation();
                int x = (int) current.getX();
                int y = (int) current.getY();

                // Move slightly, then back — keeps cursor position effectively unchanged
                robot.mouseMove(x + NUDGE_PIXELS, y);
                robot.mouseMove(x, y);

                System.out.println("[" + LocalDateTime.now().format(TIME_FORMAT)
                        + "] Activity nudge sent (cursor at " + x + "," + y + ")");
            } catch (Exception e) {
                System.err.println("Nudge failed: " + e.getMessage());
            }
        };

        // Run once immediately, then repeat every INTERVAL_MINUTES
        scheduler.scheduleAtFixedRate(nudgeTask, 0, INTERVAL_MINUTES, TimeUnit.MINUTES);

        // Keep the main thread alive since the scheduler thread is a daemon
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down SystemActivityKeeper.");
            scheduler.shutdownNow();
        }));

        try {
            Thread.currentThread().join(); // block forever until process is killed
        } catch (InterruptedException ignored) {
        }
    }
}
