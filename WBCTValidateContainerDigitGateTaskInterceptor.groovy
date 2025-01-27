package WBCT

import com.navis.argo.business.api.ArgoUtils
import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageLevel
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import com.navis.road.business.workflow.TransactionAndVisitHolder
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 
*
*  Requirements: Throws the error message, if the ctr is invalid.
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTValidateContainerDigitGateTaskInterceptor
*     Code Extension Type: GATE_TASK_INTERCEPTOR
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTValidateContainerDigitGateTaskInterceptor extends AbstractGateTaskInterceptor {

    @Override
    void execute(TransactionAndVisitHolder inTran) {
        logger.warn("executing...")

        executeInternal(inTran)

        TruckVisitDetails tv = inTran.getTv()
        if (tv != null) {
            TruckTransaction truckTran = inTran.getTran()
            logger.warn("truck tran " + truckTran)
            if (truckTran != null) {
                String eqId = truckTran.getTranCtrNbr()
                logger.warn("eqId " + eqId)
                if (ArgoUtils.isNotEmpty(eqId)) {
                    String isValidCtr = truckTran.getTranFlexString03()
                    logger.warn("is valid ctr " + isValidCtr)
                    if (ArgoUtils.isNotEmpty(isValidCtr) && IS_VALID.equals(isValidCtr)) {
                        getMessageCollector().appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf("INVALID_CONTAINER_AT_OUTGATE"), null, eqId)
                    }
                }
            }
        }
    }

    private static final String IS_VALID = "N"
    private static final Logger logger = Logger.getLogger(WBCTValidateContainerDigitGateTaskInterceptor.class);

}
