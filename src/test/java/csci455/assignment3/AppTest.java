package csci455.assignment3;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
public class AppTest {
    @Test
    public void testMethod1()
    {
        String test = "Testing Environment";
        Assertions.assertEquals("Testing Environment", test);
    }
}
