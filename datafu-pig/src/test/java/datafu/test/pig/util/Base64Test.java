package datafu.test.pig.util;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class Base64Test extends PigTests {

    /**

     define Base64Encode datafu.pig.util.Base64Encode();

     data_in = LOAD 'input' as (val:chararray);

     data_out = FOREACH data_in GENERATE Base64Encode(val) as val;

     STORE data_out INTO 'output';
     */
    @Multiline private String base64EncodeTest;

    @Test
    public void base64EncodeTest() throws Exception
    {
        PigTest test = createPigTestFromString(base64EncodeTest);

        writeLinesToFile("input",
                "hello",
                "lad",
                "how",
                "are you?"
        );

        test.runScript();

        assertOutput(test, "data_out",
                "(aGVsbG8=)",
                "(bGFk)",
                "(aG93)",
                "(YXJlIHlvdT8=)");
    }

    /**

     define Base64Decode datafu.pig.util.Base64Decode();

     data_in = LOAD 'input' as (val:chararray);

     data_out = FOREACH data_in GENERATE Base64Decode(val) as val;

     STORE data_out INTO 'output';
     */
    @Multiline private String base64DecodeTest;

    @Test
    public void base64DecodeTest() throws Exception
    {
        PigTest test = createPigTestFromString(base64DecodeTest);

        writeLinesToFile( "input",
                "aGVsbG8=",
                "bGFk",
                "aG93",
                "YXJlIHlvdT8="
        );

        test.runScript();

        assertOutput(test, "data_out",
                "(hello)",
                "(lad)",
                "(how)",
                "(are you?)");
    }

}
