package ca.on.oicr.pde.deciders.data;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 * @author mlaszloffy
 */
public class BasesMaskTest {

    @Test
    public void testStringParsing() {
        //dual barcode
        Assert.assertEquals(BasesMask.fromStringUnchecked("y1n1,i1n1,i1n1,y1n1").toString(), "y1n1,i1n1,i1n1,y1n1");
        Assert.assertEquals(BasesMask.fromStringUnchecked("y*n0,i1n1,i1n1,y*n0").toString(), "y*,i1n1,i1n1,y*");
        Assert.assertEquals(BasesMask.fromStringUnchecked("y*n0,i6n0,i6n0,y*n0").toString(), "y*,i6,i6,y*");
        Assert.assertEquals(BasesMask.fromStringUnchecked("y*,i6n*,i6n*,y*").toString(), "y*,i6n*,i6n*,y*");
        Assert.assertEquals(BasesMask.fromStringUnchecked("y*,i6n2,i6n2,y*").toString(), "y*,i6n2,i6n2,y*");
        Assert.assertEquals(BasesMask.fromStringUnchecked("y*,i6,i6,y*").toString(), "y*,i6,i6,y*");

        //single barcode
        Assert.assertEquals(BasesMask.fromStringUnchecked("y1n1,i1n1,y1n1").toString(), "y1n1,i1n1,y1n1");
        Assert.assertEquals(BasesMask.fromStringUnchecked("y*n0,i1n1,y*n0").toString(), "y*,i1n1,y*");
        Assert.assertEquals(BasesMask.fromStringUnchecked("y*n0,i6n0,y*n0").toString(), "y*,i6,y*");
        Assert.assertEquals(BasesMask.fromStringUnchecked("y*,i6n*,y*").toString(), "y*,i6n*,y*");
        Assert.assertEquals(BasesMask.fromStringUnchecked("y*,i6n2,y*").toString(), "y*,i6n2,y*");
        Assert.assertEquals(BasesMask.fromStringUnchecked("y*,i6,y*").toString(), "y*,i6,y*");
    }

}
