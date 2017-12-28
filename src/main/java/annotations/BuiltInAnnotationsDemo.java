package annotations;

import java.util.List;

/**
 * BuiltInAnnotationsDemo
 *
 * @author lyq
 * @create 2017-12-26 10:07
 */
public class BuiltInAnnotationsDemo {
    /**
     * @deprecated javadoc syntax
     */
    @Deprecated
    void foo(){
        System.out.println("this foo method is deprecated");
    }


    @SuppressWarnings("deprecation")
    void bar(){
        foo();
    }


    //@Override
    void overRideDemo(){
        System.out.println("over ride from object");
    }

    
}
