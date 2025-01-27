package WBCT

import com.navis.argo.util.XmlContentFilter
import com.navis.argo.util.XmlUtil
import com.navis.external.services.AbstractGeneralNoticeCodeExtension
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.road.business.adaptor.thirdparty.gos.GosAdaptor
import com.navis.road.business.api.RoadManager
import com.navis.road.business.apihandler.GateApiXmlUtil
import com.navis.road.business.atoms.TranStatusEnum
import com.navis.road.business.atoms.TruckVisitStatusEnum
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import com.navis.road.business.reference.CancelReason
import com.navis.services.business.event.Event
import com.navis.services.business.event.GroovyEvent
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

class WBCTNotifyGosCancellationGeneralNotice extends AbstractGeneralNoticeCodeExtension {

    @Override
    void execute(GroovyEvent inGroovyEvent) {
        LOGGER.warn("WBCTNotifyGosCancellationGeneralNotice executing...")
        TruckVisitDetails tv = inGroovyEvent.getEntity()
        LOGGER.warn("tv " + tv)
        LOGGER.warn("tv status " + tv.getTvdtlsStatus())
        LOGGER.warn("Event note " + inGroovyEvent?.getEvent()?.eventNote)

        Event event = inGroovyEvent.getEvent()
        if (event != null) {
            String notes = event.getEventNote()
            if (notes != null) {
                TruckTransaction truckTransaction = TruckTransaction.hydrate(Long.valueOf(notes))
                LOGGER.warn("truck transaction " + truckTransaction)
                LOGGER.warn("truck transaction status " + truckTransaction.getTranStatus())
                if (truckTransaction.isReceival()) {
                    Set<TruckTransaction> truckTransSet = tv.getTvdtlsTruckTrans()
                    if (truckTransSet != null && truckTransSet.size() > 1) {
                        for (TruckTransaction tran : truckTransSet) {
                            LOGGER.warn("tran " + tran)
                            if (tran.isDelivery()) {
                                LOGGER.warn("tran is dual delivery...")
                                if (!TranStatusEnum.CANCEL.equals(tran.getTranStatus())) {
                                    CancelReason cancelReason = CancelReason.findOrCreate("RECT_CANCEL_TRN", "Rectifying Unit Cancels gate transaction.")
                                    RoadManager roadManager = (RoadManager) Roastery.getBean(RoadManager.BEAN_ID);
                                    roadManager.cancelTruckTransaction(tran, cancelReason, false)
                                    if (!TruckVisitStatusEnum.CANCEL.equals(tv.getTvdtlsStatus())) {
                                        tv.cancelTruckVisitAndTransactions()
                                    }
                                }
                            }
                            HibernateApi.getInstance().save(tran)
                            HibernateApi.getInstance().flush()
                        }
                    }
                    // GosAdaptor.callGos(getCancelElement(truckTransaction, (XmlContentFilter) null))
                }

                if (truckTransaction != null) {
                    GosAdaptor.callGos(getCancelElement(truckTransaction, (XmlContentFilter) null))
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
        logMsg(XmlUtil.toString(cancelElement, true));
        return cancelElement;

    }

    private void logMsg(Object inMsg) {
        LOGGER.debug(inMsg);
    }
    private static final Logger LOGGER = Logger.getLogger(WBCTNotifyGosCancellationGeneralNotice.class)

}
