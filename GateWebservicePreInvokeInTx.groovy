import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.model.Complex
import com.navis.argo.business.model.Operator
import com.navis.argo.business.reference.Equipment
import com.navis.argo.util.ArgoGroovyUtils
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.units.Unit
import com.navis.road.RoadField
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import org.apache.log4j.Logger
import org.jdom.Element

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date:02-Jan-2025
*
*  Requirements:WBCT - 389 - validates the ctr at outgate and updates the flex string
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: GateWebservicePreInvokeInTx
*     Code Extension Type: GROOVY_PLUGIN
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class GateWebservicePreInvokeInTx {

    public void preHandlerInvoke(Map parameter) {
        log.warn("GateWebservicePreInvokeInTx executing...")
        Element element = (Element) parameter.get(ArgoGroovyUtils.WS_ROOT_ELEMENT);
        Element processTruckElement = element.getChild(PROCESS_TRUCK) != null ?
                (Element) element.getChild(PROCESS_TRUCK) : null;
        if (processTruckElement != null) {
            Element stageIdElement = processTruckElement.getChild(STAGE_ID) != null ?
                    (Element) processTruckElement.getChild(STAGE_ID) : null;
            if (stageIdElement != null) {
                if (OUTGATE.equalsIgnoreCase(stageIdElement.getText())) {
                    Element equipmentElement = processTruckElement.getChild(EQUIPMENT)
                    if (equipmentElement != null) {
                        List<Element> containerElementList = equipmentElement.getChildren(CONTAINER)
                        if (containerElementList != null && containerElementList.size() > 0) {
                            for (Element containerElement : containerElementList) {
                                if (containerElement != null) {
                                    String tvKey = processTruckElement.getChild("truck-visit")?.getAttributeValue("tv-key")
                                    String eqId = containerElement.getAttributeValue(EQ_ID)
                                    if (ArgoUtils.isNotEmpty(tvKey) && ArgoUtils.isNotEmpty(eqId)) {
                                        TruckVisitDetails truckVisitDetails = TruckVisitDetails.findTVActiveByGkey(Long.parseLong(tvKey))
                                        if (truckVisitDetails != null) {
                                            TruckTransaction transaction = validateContainer(truckVisitDetails, eqId)
                                            log.warn("transaction " + transaction)
                                            if (transaction != null) {
                                                transaction.setFieldValue(RoadField.TRAN_FLEX_STRING03, "N")
                                            }
                                            HibernateApi.getInstance().save(transaction)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private TruckTransaction validateContainer(TruckVisitDetails truckVisitDetails, String eqId) {
        if (truckVisitDetails != null && ArgoUtils.isNotEmpty(eqId)) {
            UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID)

/*            Equipment equipment = Equipment.findEquipment(eqId)
            Complex complex = Complex.findComplex(COMPLEX, Operator.findOperator(OPERATOR))
            if (equipment != null) {
                Unit unit = unitFinder.findActiveUnit(complex, equipment)*/
            Set<TruckTransaction> truckTransactionSet = truckVisitDetails.getActiveTransactions()
            if (truckTransactionSet != null && truckTransactionSet.size() > 0) {
                for (TruckTransaction transaction : truckTransactionSet) {
                    Complex complex = Complex.findComplex(COMPLEX, Operator.findOperator(OPERATOR))
                    Equipment tranEquip = transaction.getTranEq()
                    Unit tranUnit = unitFinder.findActiveUnit(complex, tranEquip)
                    log.warn("eq id " + eqId)
                    log.warn("tran unit " + tranUnit)
                    log.warn("eq length " + eqId.length())
                    if (eqId.length() != 11 && tranUnit != null && !eqId.equals(tranUnit.getUnitId())) {
                        log.warn("condition satifies...")
                        return transaction
                    }
                }
                //  }
            }
        }
    }

    private final static String STAGE_ID = "stage-id"
    private final static String PROCESS_TRUCK = "process-truck"
    private final static String EQUIPMENT = "equipment"
    private final static String CONTAINER = "container"
    private final static String EQ_ID = "eqid"
    private final static String OUTGATE = "outgate"
    private final static String OPERATOR = "PORTSAMERICA"
    private final static String COMPLEX = "USLAX"
    private final static Logger log = Logger.getLogger(GateWebservicePreInvokeInTx.class)
}