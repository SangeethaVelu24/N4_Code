import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.reference.EquipType
import com.navis.argo.util.XmlUtil
import com.navis.external.argo.AbstractArgoCustomWSHandler
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.BizViolation
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageCollector
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.orders.business.eqorders.Booking
import com.navis.road.RoadEntity
import com.navis.road.RoadField
import com.navis.road.business.atoms.TranSubTypeEnum
import com.navis.road.business.model.TruckTransaction
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.jdom.Element
import org.jdom.Namespace

/**
 * @Author: weservetech.com; Date: 19-Dec-2024
 *
 *  Requirements: The groovy is used for TWP integration to handle the web service request that provides details about Booking and units received/delivered.
 *
 * @Inclusion Location: Incorporated as a code extension of the type
 *
 *  Load Code Extension to N4:
 *  1. Go to Administration --> System --> Code Extensions
 *  2. Click Add (+)
 *  3. Enter the values as below:
 *     Code Extension Name: PAUnitReceivedForBookingHandler
 *     Code Extension Type: WS_ARGO_CUSTOM_HANDLER
 *     Groovy Code: Copy and paste the contents of groovy code.
 *  4. Click Save button
 *
 *   S.No    Modified Date       Modified By             Jira             Description
 *   1.     19-Dec-2024         Sangeetha Velu      WBCT - 370      Included the isWheeled, seal-number, type attribute in the response.
 *
 */


class PAUnitReceivedForBookingHandler extends AbstractArgoCustomWSHandler {
    private static final Logger logger = Logger.getLogger(PAUnitReceivedForBookingHandler.class)

    @Override
    void execute(UserContext userContext, MessageCollector mc, Element inElement, Element inOutEResponse, Long aLong) {
        if (inElement.getChildren(BOOKING_INQUIRY) != null && inElement.getChildren(BOOKING_INQUIRY).size() > 0) {
            Element responseRoot = new Element(BOOKINGS)
            inOutEResponse.addContent(responseRoot)

            List<Element> bkgDetailsElement = inElement.getChildren(BOOKING_INQUIRY)
            for (Element currentBKGElement : bkgDetailsElement) {

                Element bkgElement = currentBKGElement.getChild(BOOKING)
                String containerNbr
                if (bkgElement != null) {
                    String bkgGkey = bkgElement.getAttributeValue(GKEY)
                    if (!StringUtils.isEmpty(bkgGkey)) {
                        Booking currentBooking = (Booking) HibernateApi.getInstance().get(Booking.class, Long.parseLong(bkgGkey));
                        if (currentBooking != null) {
                            Collection units = getFinder().findUnitsForOrder(currentBooking)
                            if (units != null && !units.isEmpty()) {
                                Element resElement = createEqoElement(currentBooking, units)
                                responseRoot.addContent(resElement)
                            }
                        } else {
                            throw BizViolation.create(PropertyKeyFactory.keyWithFormat(BOOKING_NOT_FOUND, BOOKING_NOT_FOUND), null)
                        }
                    } else {
                        throw BizViolation.create(PropertyKeyFactory.keyWithFormat(BOOKING_NOT_PROVIDED, BOOKING_NOT_PROVIDED), null)
                    }
                }
            }
        }
    }

    Element createEqoElement(inBkg, Collection inUnits) {
        Element inBkgInfo = new Element(ORDER)
        XmlUtil.setOptionalAttribute(inBkgInfo, GKEY, inBkg.getPrimaryKey(), (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, NBR, inBkg.getEqboNbr(), (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, SSCO, inBkg.getEqoLine() != null ? inBkg.getEqoLine().getBzuId() : "", (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, TALLY_IN, inBkg.getEqoTallyReceive() != null ? inBkg.getEqoTallyReceive() : "", (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, TALLY_OUT, inBkg.getEqoTally() != null ? inBkg.getEqoTally() : "", (Namespace) null);


        Element unitsElement = new Element(UNITS)
        inBkgInfo.addContent(unitsElement)
        if (inUnits != null && !inUnits.isEmpty()) {
            for (Unit unit : inUnits) {
                Element element = new Element(UNIT)
                unitsElement.addContent(element)
                String yardLocation = ""
                String inTime = ""
                String outTime = ""
                String transitState = ""

                String truckerCode = ""
                String truckerName = ""
                boolean isWheeled = false
                EquipType equipType = null
                String sealNo = ""
                if (unit != null) {
                    UnitFacilityVisit ufv = unit.getUfvForFacilityNewest(ContextHelper.getThreadFacility())

                    if (ufv != null) {
                        yardLocation = ufv.isTransitState(UfvTransitStateEnum.S40_YARD) ? ufv.getUfvLastKnownPosition() : "CARRIER"
                        inTime = ufv.getUfvTimeIn() != null ? ufv.getUfvTimeIn().format("yyyy-MM-dd hh:mm:ss").toString() : ""
                        outTime = ufv.getUfvTimeOut() != null ? ufv.getUfvTimeOut().format("yyyy-MM-dd hh:mm:ss").toString() : ""
                        transitState = ufv.getUfvTransitState() != null ? ufv.getUfvTransitState().getKey().substring(4) : ""
                        TruckTransaction tran = null
                        if (ufv.isTransitStateAtLeast(UfvTransitStateEnum.S40_YARD) && unit.getUnitCategory() == UnitCategoryEnum.EXPORT) {
                            tran = findLatestTransactionForUnit(TranSubTypeEnum.RE, unit)
                            if (tran && tran.getTranTruckVisit() != null && tran.getTranTruckVisit().getTvdtlsTrkCompany() != null) {
                                truckerCode = tran.getTranTruckVisit().getTvdtlsTrkCompany().getBzuId()
                                truckerName = tran.getTranTruckVisit().getTvdtlsTrkCompany().getBzuName()
                            }
                        } else if (ufv.isTransitStateBeyond(UfvTransitStateEnum.S40_YARD) && unit.getUnitCategory() == UnitCategoryEnum.STORAGE) {
                            tran = findLatestTransactionForUnit(TranSubTypeEnum.DM, unit)
                            if (tran && tran.getTranTruckVisit() != null && tran.getTranTruckVisit().getTvdtlsTrkCompany() != null) {
                                truckerCode = tran.getTranTruckVisit().getTvdtlsTrkCompany().getBzuId()
                                truckerName = tran.getTranTruckVisit().getTvdtlsTrkCompany().getBzuName()
                            }
                        }
                        if (ArgoUtils.isNotEmpty(ufv?.getUfvFlexString02())) {
                            if (IS_WHEELED.equals(ufv.getUfvFlexString02())) {
                                isWheeled = true
                            }
                        }
                        equipType = unit?.getPrimaryEq()?.getEqEquipType()
                        if (ArgoUtils.isNotEmpty(unit.getUnitSealNbr1())) {
                            sealNo = unit.getUnitSealNbr1()
                        } else if (ArgoUtils.isNotEmpty(unit.getUnitSealNbr2())) {
                            sealNo = unit.getUnitSealNbr2()
                        } else if (ArgoUtils.isNotEmpty(unit.getUnitSealNbr3())) {
                            sealNo = unit.getUnitSealNbr3()
                        } else if (ArgoUtils.isNotEmpty(unit.getUnitSealNbr4())) {
                            sealNo = unit.getUnitSealNbr4()
                        }
                    }


                    if (unit != null) {
                        XmlUtil.setOptionalAttribute(element, GKEY, unit.getPrimaryKey(), (Namespace) null);
                        XmlUtil.setOptionalAttribute(element, UNIT_ID, unit.getUnitId() != null ? unit.getUnitId() : "", (Namespace) null);
                        XmlUtil.setOptionalAttribute(element, YARD_LOCATION, yardLocation, (Namespace) null);
                        XmlUtil.setOptionalAttribute(element, IN_TIME, inTime, (Namespace) null);
                        XmlUtil.setOptionalAttribute(element, OUT_TIME, outTime, (Namespace) null);
                        XmlUtil.setOptionalAttribute(element, TRUCKER_CODE, truckerCode, (Namespace) null);
                        XmlUtil.setOptionalAttribute(element, TRUCKER_NAME, truckerName, (Namespace) null);
                        XmlUtil.setOptionalAttribute(element, T_STATE, transitState, (Namespace) null);
                        XmlUtil.setOptionalAttribute(element, TYPE, equipType != null ? equipType?.getEqtypId() : "", (Namespace) null);
                        XmlUtil.setOptionalAttribute(element, SEAL_NUMBER, sealNo, (Namespace) null);
                        XmlUtil.setOptionalAttribute(element, IS__WHEELED, isWheeled, (Namespace) null);

                    }
                }

            }
        }
        return inBkgInfo
    }


    TruckTransaction findLatestTransactionForUnit(TranSubTypeEnum inTranSubType, Unit unit) {
        DomainQuery dq = QueryUtils.createDomainQuery(RoadEntity.TRUCK_TRANSACTION)
                .addDqPredicate(PredicateFactory.eq(RoadField.TRAN_SUB_TYPE, inTranSubType))
                .addDqPredicate(PredicateFactory.eq(RoadField.TRAN_UNIT, unit.getUnitGkey()))
                .addDqOrdering(Ordering.desc(RoadField.TRAN_GKEY))
                .setDqMaxResults(1)
        return (TruckTransaction) HibernateApi.getInstance().getUniqueEntityByDomainQuery(dq);
    }

    private UnitFinder getFinder() {
        return (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);
    }
    private static final String BOOKING_INQUIRY = "booking-inquiry"
    private static final String BOOKINGS = "bookings"
    private static final String BOOKING = "booking"
    private static final String BOOKING_NOT_FOUND = "BOOKING_NOT_FOUND"
    private static final String BOOKING_NOT_PROVIDED = "BOOKING_NOT_PROVIDED"
    private static final String GKEY = "gkey"
    private static final String ORDER = "order"
    private static final String SSCO = "ssco"
    private static final String TALLY_IN = "tally-in"
    private static final String TALLY_OUT = "tally-out"
    private static final String NBR = "nbr"
    private static final String UNITS = "units"
    private static final String UNIT = "unit"
    private static final String UNIT_ID = "unit-id"
    private static final String YARD_LOCATION = "yard-location"
    private static final String IN_TIME = "in-time"
    private static final String OUT_TIME = "out-time"
    private static final String TRUCKER_CODE = "trucker-code"
    private static final String TRUCKER_NAME = "trucker-name"
    private static final String T_STATE = "t-state"
    private static final String TYPE = "type"
    private static final String SEAL_NUMBER = "seal-number"
    private static final String IS__WHEELED = "is-wheeled"
    private static final String IS_WHEELED = "W"
}