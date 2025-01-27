package WBCT;

/*
 * @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date:
 *
 *  Requirements:
 *
 *
 * @Inclusion Location: Incorporated as a code extension of the type
 *
 *  Load Code Extension to N4:
 *  1. Go to Administration --> System --> Code Extensions
 *  2. Click Add (+)
 *  3. Enter the values as below:
 *     Code Extension Name:
 *     Code Extension Type:
 *  4. Click Save button
 *
 *  @Setup:
 *
 *  S.No    Modified Date   Modified By     Jira         Description
 *
 */
public class TestCreation {

    public class Block{

        public class Bay{

        }
    }

    class main{
        public static void main(String[] args){
            TestCreation testCreation = new TestCreation();

            TestCreation.Block block = testCreation.new Block();
            Block.Bay bay = block.new Bay();
        }
    }

}
