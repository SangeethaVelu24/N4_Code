import com.navis.argo.util.XmlContentFilter
import com.navis.argo.util.XmlUtil
import com.navis.road.business.adaptor.thirdparty.gos.GosAdaptor
import com.navis.road.business.apihandler.GateApiXmlUtil
import com.navis.road.business.model.TruckTransaction
import org.apache.log4j.Logger
import org.jdom.Element
import org.jdom.Namespace

/*
*Author <a href="mailto:gtharani@weservetech.com">Tharani G</a>,10/09/2024
*
* Requirement : [WBCT-81] --> Gate-10 Notify GOS of Cancellations
*
* @Inclusion Location: Incorporated as a code extension of the type LIBRARY
*
* Load Code Extension to N4:
         *  1. Go to Administration --> System --> Code Extensions
         *  2. Click Add (+)
         *  3. Enter the values as below:
             Code Extension Name: PASendGOSCancelTranLibrary
             Code Extension Type: LIBRARY
             Groovy Code: Copy and paste the contents of groovy code.
         *  4. Click Save button
*  S.No    Modified Date   Modified By              Jira      Description
*   1      08/Oct/2024     rgopal@weservetech.com   WBCT-81   Send GOS message for all transactions, If truckvisit have multiple transactions.
*/

class PASendGOSCancelTranLibrary {
    private static final Logger LOGGER = Logger.getLogger(PASendGOSCancelTranLibrary.class);

    public void logMsg(String message) {
        LOGGER.warn("PASendGOSCancelTranLibrary: " + message);
    }

    public void execute(TruckTransaction inTran) {
        if (inTran != null) {
            if (inTran.isDelivery()){
                GosAdaptor.callGos(getCancelElement(inTran, (XmlContentFilter) null));
            }else {
                Set<TruckTransaction> transactionSet = inTran != null ? inTran.getTranTruckVisit()?.getActiveTransactions() : null
                if (transactionSet != null && !transactionSet.isEmpty()) {
                    for (TruckTransaction transaction : transactionSet) {
                        GosAdaptor.callGos(getCancelElement(transaction, (XmlContentFilter) null));
                    }
                }
            }
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
        LOGGER.warn(XmlUtil.toString(cancelElement, true));
        return cancelElement;
    }
}