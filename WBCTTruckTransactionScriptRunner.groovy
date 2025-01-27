package WBCT

import com.navis.road.business.model.TruckVisitDetails

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

class WBCTTruckTransactionScriptRunner {
    String execute(){
        StringBuilder sb = new StringBuilder()

        Long tvKey = 493781579
        TruckVisitDetails truckVisitDetails = TruckVisitDetails.findTruckVisitByGkey(tvKey)
        sb.append("tv "+truckVisitDetails).append("\n")

        return sb.toString()
    }
}
