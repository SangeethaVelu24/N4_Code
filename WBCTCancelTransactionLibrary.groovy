package WBCT

import com.navis.argo.business.api.IEvent
import com.navis.argo.business.api.ServicesManager
import com.navis.argo.util.XmlContentFilter
import com.navis.argo.util.XmlUtil
import com.navis.external.framework.AbstractExtensionCallback
import com.navis.framework.business.Roastery
import com.navis.road.business.apihandler.GateApiXmlUtil
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import com.navis.services.business.rules.EventType
import org.apache.log4j.Logger
import org.jdom.Element
import org.jdom.Namespace

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

class WBCTCancelTransactionLibrary extends AbstractExtensionCallback {

    void cancelTransaction(TruckTransaction tran) {
        LOGGER.warn("WBCTCancelTransactionLibrary cancelTransaction executing...")
        if (tran != null) {
            TruckVisitDetails truckVisitDetails = tran.getTranTruckVisit()
            if (truckVisitDetails != null) {
                EventType eventType = EventType.findEventType("NOTIFY_CANCEL_MSG")

                ServicesManager servicesManager = (ServicesManager) Roastery.getBean(ServicesManager.BEAN_ID)
                IEvent event = servicesManager.getMostRecentEvent(eventType, truckVisitDetails)
                LOGGER.warn("event " + event)
                LOGGER.warn("has one cancel transaction "+truckVisitDetails.hasOneCancelTransaction())
                if (!truckVisitDetails.hasCancelTransaction() &&  event == null) {
                    truckVisitDetails.recordTruckVisitEvent(eventType, null, "Gos Cancel Notify Msg")
                }
            }

            int cancelCount = 0;
            /*     Set<TruckTransaction> truckTrans = visitDetails.getTvdtlsTruckTrans()
                 for (TruckTransaction truckTran : (truckTrans as List<TruckTransaction>)) {
                     if (tran.getTranGkey() != truckTran.getTranGkey()) {
                         switch (truckTran.getTranStatus()) {
                             case TranStatusEnum.CANCEL:
                                 cancelCount = cancelCount + 1;
                                 break;
                         }
                     }
                 }*/

            /* TruckTransaction ttran = (TruckTransaction) Roastery.getHibernateApi().load(TruckTransaction.class, tran.getPrimaryKey());
             LOGGER.warn("tran " + ttran)
             LOGGER.warn("tran status " + ttran.getTranStatus())
             if (ttran != null) {
                 try {
                     if (ttran != null) {
                         if (ttran.isDelivery()) {
                             GosAdaptor.callGos(getCancelElement(ttran, (XmlContentFilter) null));
                         } else {
                             Set<TruckTransaction> transactionSet = ttran != null ? ttran.getTranTruckVisit()?.getActiveTransactions() : null
                             LOGGER.warn("transaction set " + transactionSet)
                             if (transactionSet != null && !transactionSet.isEmpty()) {
                                 for (TruckTransaction transaction : transactionSet) {
                                     LOGGER.warn("transaction status for receival " + transaction.getTranStatus())
                                     GosAdaptor.callGos(getCancelElement(transaction, (XmlContentFilter) null));
                                 }
                             }
                         }
                     }
                 } catch (Exception e) {
                     logMsg("Exception while sending cancel " + e.toString());
                 }
             }*/
        }
    }

    private Element getCancelElement(TruckTransaction inTran, XmlContentFilter inCF) {
        Element cancelElement = new Element("cancel-transaction");
        Namespace ns = cancelElement.getNamespace();
        Element eTruckVisit = GateApiXmlUtil.createBasicTruckVisitElement(inTran.getTranTruckVisit(), ns, inCF);
        Element eTruckTransactions = new Element("truck-transactions");
        Element eTran = GateApiXmlUtil.createBasicTruckTransactionElement(inTran, inCF);
        eTruckTransactions.addContent(eTran);
        eTruckVisit.addContent(eTruckTransactions);
        cancelElement.addContent(eTruckVisit);
        logMsg(XmlUtil.toString(cancelElement, true));
        return cancelElement;
    }

    private void logMsg(Object inMsg) {
        LOGGER.debug(inMsg);
    }

    private static final Logger LOGGER = Logger.getLogger(WBCTCancelTransactionLibrary.class)

}
