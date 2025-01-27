import com.navis.argo.util.XmlContentFilter
import com.navis.argo.util.XmlUtil
import com.navis.external.framework.persistence.AbstractExtensionPersistenceCallback
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.hibernate.CarinaPersistenceCallback
import com.navis.framework.persistence.hibernate.PersistenceTemplate
import com.navis.road.business.adaptor.thirdparty.gos.GosAdaptor
import com.navis.road.business.apihandler.GateApiXmlUtil
import com.navis.road.business.atoms.TranStatusEnum
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import com.navis.services.business.rules.EventType
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.jdom.Element
import org.jdom.Namespace
import org.jetbrains.annotations.Nullable

/*
*Author <a href="mailto:gtharani@weservetech.com">Tharani G</a>,10/Sep/2024
*
* Requirement :  [WBCT-81] --> Gate-10 Notify GOS of Cancellations
*
* @Inclusion Location: Incorporated as a code extension of the type TRANSACTED_BUSINESS_FUNCTION
*
* Load Code Extension to N4:
         *  1. Go to Administration --> System --> Code Extensions
         *  2. Click Add (+)
         *  3. Enter the values as below:
             Code Extension Name: PAGateTranCancelFormTransactionBusiness
             Code Extension Type: TRANSACTED_BUSINESS_FUNCTION
             Groovy Code: Copy and paste the contents of groovy code.
         *  4. Click Save button
* S.No    Modified Date   Modified By         Jira        Description
* 1.      21-Oct-2024    Sangeetha Velu      WBCT-81   sends the cancel message to GOS
*/

class PAGateTranCancelFormTransactionBusiness extends AbstractExtensionPersistenceCallback {

    @Override
    void execute(@Nullable Map inParms, @Nullable Map inOutResults) {
        LOGGER.setLevel(Level.DEBUG);
        logMsg("Started");
        if (inParms[MAP_KEY] != null) {
            List<Serializable> gkeyList = inParms.get(MAP_KEY) as List;
            PersistenceTemplate pt = new PersistenceTemplate(getUserContext());

            pt.invoke(new CarinaPersistenceCallback() {
                @Override
                protected void doInTransaction() {
                    for (Serializable evntGkey : gkeyList) {
                        LOGGER.warn("evnt gkey " + evntGkey)
                        TruckTransaction ttran = (TruckTransaction) Roastery.getHibernateApi().load(TruckTransaction.class, evntGkey);
                        LOGGER.warn("ttran " + ttran)
                        LOGGER.warn("tran status " + ttran?.getTranStatus())
                        if (ttran != null) {
                            try {
                                TruckVisitDetails tv = ttran.getTranTruckVisit()
                                if (tv != null) {
                                    EventType eventType = EventType.findEventType("CUSTOM_TRAN_CANCEL")
                                    if (eventType) {
                                        tv.recordTruckVisitEvent(eventType, null, String.valueOf(ttran.getTranGkey()))
                                    }
                                }

//                                if (ttran != null) {
//                                    EventType eventType = EventType.findEventType("CUSTOM_TRAN_CANCEL")
//                                    if (ttran.isReceival()) {
//                                        Set<TruckTransaction> transactionSet = ttran != null ? ttran.getTranTruckVisit()?.getActiveTransactions() : null
//                                        if (transactionSet != null && !transactionSet.isEmpty()) {
//                                            for (TruckTransaction transaction : transactionSet) {
//
//                                                TruckVisitDetails tv = transaction.getTranTruckVisit()
//                                                if (tv != null) {
//                                                    tv.recordEvent(eventType, null, String.valueOf(transaction?.getTranNbr()), ArgoUtils.timeNow())
//                                                }
//                                                    GosAdaptor.callGos(getCancelElement(transaction, (XmlContentFilter) null));
//                                            }
//                                        }
//                                        return
//                                    } else {
//                                          Set<TruckTransaction> transactionSet = ttran != null ? ttran.getTranTruckVisit()?.getActiveTransactions() : null
//                                          if (transactionSet != null && !transactionSet.isEmpty()) {
//                                              for (TruckTransaction transaction : transactionSet) {
//                                                  GosAdaptor.callGos(getCancelElement(transaction, (XmlContentFilter) null));
//                                              }
//                                          }
//                                        TruckVisitDetails tv = ttran.getTranTruckVisit()
//                                        if (tv != null) {
//                                            tv.recordEvent(eventType, null, String.valueOf(ttran?.getTranNbr()), ArgoUtils.timeNow())
//                                        }
//                                        GosAdaptor.callGos(getCancelElement(ttran, (XmlContentFilter) null));
//                                    }
//                                }
                            } catch (Exception e) {
                                logMsg("Exception while sending cancel " + e.toString());
                            }
                        }
                    }
                }
            });
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

    private static final String MAP_KEY = 'gkeys';
    private static final Logger LOGGER = Logger.getLogger(PAGateTranCancelFormTransactionBusiness.class)
}

