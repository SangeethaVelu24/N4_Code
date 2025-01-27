import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.reference.EquipType
import com.navis.argo.util.XmlUtil
import com.navis.external.argo.AbstractArgoCustomWSHandler
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageCollector
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.InventoryField
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.orders.OrdersEntity
import com.navis.orders.business.eqorders.Booking
import com.navis.orders.business.eqorders.EquipmentOrderItem
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.jdom.Element
import org.jdom.Namespace

/**
 * @Author: weservetech.com; Date: 19-Dec-2024
 *
 *  Requirements: The groovy is used for TWP integration to handle the web service request that provides details of booking and units associated.
 *
 * @Inclusion Location: Incorporated as a code extension of the type
 *
 *  Load Code Extension to N4:
 *  1. Go to Administration --> System --> Code Extensions
 *  2. Click Add (+)
 *  3. Enter the values as below:
 *     Code Extension Name:PACBRBookingInquiryHandler
 *     Code Extension Type: WS_ARGO_CUSTOM_HANDLER
 *     Groovy Code: Copy and paste the contents of groovy code.
 *  4. Click Save button
 *
 *  S.No    Modified Date       Modified By             Jira             Description
 *  1.      19-Dec-2024         Sangeetha Velu        WBCT-370      Included the isWheeled attribute in response.
 */

class PACBRBookingInquiryHandler extends AbstractArgoCustomWSHandler {
    private static final Logger logger = Logger.getLogger(PACBRBookingInquiryHandler.class)

    @Override
    void execute(final UserContext uc,
                 final MessageCollector mc,
                 final Element inElement, final Element inOutEResponse, final Long wsLogKey) {
        Element rootElement = inElement.getChild(CBR_INQUIRY)
        Element parameterList = rootElement.getChild(PARAMETER)
        Element bkgElement = new Element(BOOKINGS)
        boolean isOpen = false
        if (parameterList != null) {
            Element parameter = parameterList

            if (parameter != null) {
                String bookNbrs = parameter.getAttributeValue(BOOK_NBR)
                if (bookNbrs != null && !bookNbrs.isEmpty()) {
                    List<String> errorList = new ArrayList<>()
                    String[] books = bookNbrs.split(",")*.trim()
                    if (books.size() > 50) {
                        getMessageCollector().appendMessage(MessageLevel.INFO, PropertyKeyFactory.valueOf("Inquiry received for Booking beyond limit. Please note that first 50 will be considered."), null, null)
                    }

                    if (errorList != null && !errorList.isEmpty()) {
                        //skip validation
                    } else {
                        DomainQuery dq = QueryUtils.createDomainQuery(OrdersEntity.BOOKING)
                                .addDqPredicate(PredicateFactory.in(InventoryField.EQBO_NBR, books.take(50)))

                        Serializable[] arrayBook = HibernateApi.getInstance().findPrimaryKeysByDomainQuery(dq)
                        if (arrayBook.size() == 0) {
                            mc.appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf(ORDER_NOT_FOUND), null, bookNbrs)
                        }

                        for (Serializable bookKey : arrayBook) {
                            Booking booking = Booking.hydrate(bookKey)
                            if (booking != null) {
                                CarrierVisit cv = booking?.getEqoVesselVisit()
                                if (cv != null && !cv?.isVisitPhaseActive()) {
                                    isOpen = true
                                }

                                Element orderElement = createBookingElement(booking, isOpen)
                                bkgElement.addContent(orderElement)
                            }
                        }
                        inOutEResponse.addContent(bkgElement)
                    }
                } else if (StringUtils.isEmpty(bookNbrs)) {
                    mc.appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf(BOOKING_NOT_PROVIDED), null, null)
                }
            }
        }
    }

    Element createBookingElement(inBkg, isOpen) {
        Element inBkgInfo = new Element(ORDER)
        def integrationUtil = getLibrary("PAIntegrationUtil")
        XmlUtil.setOptionalAttribute(inBkgInfo, GKEY, inBkg.getPrimaryKey(), (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo,NBR, inBkg.getEqboNbr(), (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, SSCO, inBkg.getEqoLine() != null ? inBkg.getEqoLine().getBzuId() : "", (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo,VESSEL_VISIT , inBkg.getEqoVesselVisit() != null ? inBkg.getEqoVesselVisit().getCvId() : "", (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, VESSEL_NAME, inBkg.getEqoVesselVisit() != null ? inBkg.getEqoVesselVisit().getCarrierVehicleName() : "", (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, IS_OPEN, !isOpen ? "true" : "false", (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, POL, inBkg.getEqoPol() != null ? inBkg.getEqoPol().getPointId() : "", (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, POD_1, inBkg.getEqoPod1() != null ? inBkg.getEqoPod1().getPointId() : "", (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, POD_2, inBkg.getEqoPod2() != null ? inBkg.getEqoPod2().getPointId() : "", (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, DESTINATION, inBkg.getEqoDestination(), (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, BOOKING_TYPE, inBkg.getEqboSubType(), (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, CATEGORY, inBkg.getEqoCategory() != null ? inBkg.getEqoCategory().getKey() : "", (Namespace) null);
        XmlUtil.setOptionalAttribute(inBkgInfo, FREIGHT_KIND, inBkg.getEqoEqStatus() != null ? inBkg.getEqoEqStatus() : "", (Namespace) null);
        populateOrderItems(inBkgInfo, inBkg, null)
        if (inBkg.getEqoHazards() && integrationUtil != null) {
            integrationUtil.addHazardElement(inBkgInfo, inBkg.getEqoHazards())

        }
        return inBkgInfo
    }

    void populateOrderItems(Element inBkgInfo, Booking inBkg, Set<EquipmentOrderItem> oiSet) {
        Set<EquipmentOrderItem> bkgOrderItems = inBkg.getEqboOrderItems();
        if (oiSet != null) {
            bkgOrderItems = oiSet
        }

        Element inBookingItemElement;
        if (bkgOrderItems != null && !bkgOrderItems.isEmpty()) {
            for (Iterator var4 = bkgOrderItems.iterator(); var4.hasNext(); inBkgInfo.addContent(inBookingItemElement)) {
                Object bkgOrderItem = var4.next();
                EquipmentOrderItem inEqoi = (EquipmentOrderItem) bkgOrderItem;
                inBookingItemElement = new Element(BOOKING_ITEM, inBkgInfo.getNamespace());
                XmlUtil.setOptionalAttribute(inBookingItemElement, GKEY, inEqoi.getEqboiGkey(), (Namespace) null);
                XmlUtil.setOptionalAttribute(inBookingItemElement, ISO_GROUP, inEqoi.getEqoiEqIsoGroup(), (Namespace) null);
                XmlUtil.setOptionalAttribute(inBookingItemElement, EQ_SIZE, inEqoi.getEqoiEqSize() != null ? inEqoi.getEqoiEqSize().getName().replaceAll("NOM", "") : "", (Namespace) null);
                XmlUtil.setOptionalAttribute(inBookingItemElement, EQUIPMENT_TYPE, inEqoi.getEqoiSampleEquipType() != null ? inEqoi.getEqoiSampleEquipType().getEqtypId() : "", (Namespace) null);
                XmlUtil.setOptionalAttribute(inBookingItemElement, EQ_HEIGHT, inEqoi.getEqoiEqHeight() != null ? inEqoi.getEqoiEqHeight().getName().replaceAll("NOM", "") : "", (Namespace) null);
                XmlUtil.setOptionalAttribute(inBookingItemElement, TALLY_IN, inEqoi.getEqoiTallyReceive() != null ? inEqoi.getEqoiTallyReceive() : "", (Namespace) null);
                XmlUtil.setOptionalAttribute(inBookingItemElement, TALLY_OUT, inEqoi.getEqoiTally() != null ? inEqoi.getEqoiTally() : "", (Namespace) null);
                XmlUtil.setOptionalAttribute(inBookingItemElement, QTY, inEqoi.getEqoiQty(), (Namespace) null);
                XmlUtil.setOptionalAttribute(inBookingItemElement, COMMODITY, inEqoi.getEqoiCommodity() != null ? inEqoi.getEqoiCommodity().getCmdyId() : "", (Namespace) null);
                XmlUtil.setOptionalAttribute(inBookingItemElement, IS_OOG, inEqoi.getEqoiIsOog() != null ? inEqoi.getEqoiIsOog().toString() : "", (Namespace) null);

                Collection unitsForOrderItem = getFinder().findUnitsForOrderItem(inEqoi)
                Element inUnitsInfo = new Element(UNITS)
                if (unitsForOrderItem != null && !unitsForOrderItem.isEmpty()) {
                    int count = 0
                    String yardLocation = ""
                    unitsForOrderItem.each {
                        unit ->
                            Unit unitFound = unit
                            Element inUnitInfo = new Element(UNIT)
                            if (unitFound != null) {
                                XmlUtil.setOptionalAttribute(inUnitInfo, NBR, unitFound != null ? unitFound?.getUnitId() : "", (Namespace) null);
                                EquipType equipType = unitFound?.getPrimaryEq()?.getEqEquipType()
                                XmlUtil.setOptionalAttribute(inUnitInfo, TYPE, equipType != null ? equipType?.getEqtypId() : "", (Namespace) null);
                                XmlUtil.setOptionalAttribute(inUnitInfo, EQ_SIZE, equipType != null ? equipType?.getEqtypNominalLength()?.getKey()?.replaceAll("NOM", "") : "", (Namespace) null);
                                XmlUtil.setOptionalAttribute(inUnitInfo, EQ_HEIGHT, equipType != null ? equipType?.getEqtypNominalHeight()?.getKey()?.replaceAll("NOM", "") : "", (Namespace) null);
                                XmlUtil.setOptionalAttribute(inUnitInfo, ISO_GROUP, equipType != null ? equipType?.getEqtypIsoGroup()?.getKey() : "", (Namespace) null);

                                UnitFacilityVisit ufv = unitFound?.getUnitActiveUfvNowActive()
                                boolean isWheeled = false
                                if (ufv == null) {
                                    ufv = unitFound?.getUfvForFacilityNewest(ContextHelper.getThreadFacility())
                                }
                                if (ufv != null) {
                                    if (UfvTransitStateEnum.S10_ADVISED.equals(ufv?.getUfvTransitState()) || UfvTransitStateEnum.S20_INBOUND.equals(ufv?.getUfvTransitState())) {
                                        count = count + 1
                                    }
                                    if (ArgoUtils.isNotEmpty(ufv?.getUfvFlexString02())) {
                                        if (IS_WHEELED.equals(ufv.getUfvFlexString02())) {
                                            isWheeled = true
                                        }
                                    }
                                }
                                yardLocation = ufv?.getUfvLastKnownPosition() != null ? ufv?.getUfvLastKnownPosition() : ""
                                XmlUtil.setOptionalAttribute(inUnitInfo, YARD_LOCATION, yardLocation != null ? yardLocation : "", (Namespace) null);
                                XmlUtil.setOptionalAttribute(inUnitInfo, YARD_STATUS, ufv?.getUfvTransitState() != null ? ufv?.getUfvTransitState()?.getKey()?.substring(4) : "", (Namespace) null);
                                XmlUtil.setOptionalAttribute(inUnitInfo, IS__WHEELED, isWheeled, (Namespace) null);
                            }
                            inUnitsInfo.addContent(inUnitInfo)
                    }
                    XmlUtil.setOptionalAttribute(inBookingItemElement, TOTAL_UNIT_COUNT, count > 0 ? count : "0", (Namespace) null);
                    inBookingItemElement.addContent(inUnitsInfo)
                }

                if (inEqoi.hasReeferRequirements()) {
                    Element reefElement = new Element(REEFER)
                    XmlUtil.setOptionalAttribute(inBkgInfo, HAS_REEFER, "true", (Namespace) null);
                    XmlUtil.setOptionalAttribute(reefElement, REEFER_TEMP_REQUIRED, inEqoi.getEqoiTempRequired() != null ? inEqoi.getEqoiTempRequired().round(1) : "", (Namespace) null);
                    XmlUtil.setOptionalAttribute(reefElement, REEFER_VENT_REQUIRED, inEqoi.getEqoiVentRequired() != null ? inEqoi.getEqoiVentRequired().round(1) : "", (Namespace) null);
                    XmlUtil.setOptionalAttribute(reefElement, REEFER_VENT_UNIT, inEqoi.getEqoiVentUnit() != null ? inEqoi.getEqoiVentUnit().getName() : "", (Namespace) null);
                    XmlUtil.setOptionalAttribute(reefElement, HUMIDITY, inEqoi.getEqoiHumidityRequired() != null ? inEqoi.getEqoiHumidityRequired().toString() : "", (Namespace) null);
                    XmlUtil.setOptionalAttribute(reefElement, O2, inEqoi.getEqoiO2Required() != null ? inEqoi.getEqoiO2Required().toString() : "", (Namespace) null);
                    XmlUtil.setOptionalAttribute(reefElement, CO2, inEqoi.getEqoiCo2Required() != null ? inEqoi.getEqoiCo2Required().toString() : "", (Namespace) null);
                    inBookingItemElement.addContent(reefElement)
                } else {
                    XmlUtil.setOptionalAttribute(inBkgInfo, HAS_REEFER, "false", (Namespace) null);
                }
            }
        }
    }

    private UnitFinder getFinder() {
        return (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID);
    }
    private static final String CBR_INQUIRY = "cbr-inquiry"
    private static final String BOOKINGS = "bookings"
    private static final String BOOKING_ITEM = "booking-item"
    private static final String PARAMETER = "parameter"
    private static final String ORDER_NOT_FOUND = "ORDER_NOT_FOUND"
    private static final String BOOKING_NOT_PROVIDED = "BOOKING_NOT_PROVIDED"
    private static final String GKEY = "gkey"
    private static final String ORDER = "order"
    private static final String SSCO = "ssco"
    private static final String TALLY_IN = "tally-in"
    private static final String TALLY_OUT = "tally-out"
    private static final String BOOK_NBR = "book-nbr"
    private static final String NBR = "nbr"
    private static final String UNITS = "units"
    private static final String UNIT = "unit"
    private static final String VESSEL_VISIT = "vessel-visit"
    private static final String VESSEL_NAME = "vessel-name"
    private static final String YARD_LOCATION = "yard-location"
    private static final String IS_OPEN = "is-open"
    private static final String POL = "pol"
    private static final String POD_1 = "pod-1"
    private static final String POD_2 = "pod-2"
    private static final String DESTINATION = "destination"
    private static final String TYPE = "type"
    private static final String BOOKING_TYPE = "booking-type"
    private static final String CATEGORY = "category"
    private static final String FREIGHT_KIND = "freight-kind"
    private static final String HAS_REEFER = "has-reefer"
    private static final String EQ_SIZE = "eq-size"
    private static final String EQ_HEIGHT = "eq-height"
    private static final String EQUIPMENT_TYPE = "equipment-type"
    private static final String ISO_GROUP = "iso-group"
    private static final String QTY = "qty"
    private static final String COMMODITY = "commodity"
    private static final String IS_OOG = "is-oog"
    private static final String TOTAL_UNIT_COUNT = "total-unit-count"
    private static final String YARD_STATUS = "yard-status"
    private static final String IS__WHEELED = "is-wheeled"
    private static final String REEFER = "reefer"
    private static final String REEFER_VENT_UNIT = "reefer-vent-unit"
    private static final String HUMIDITY = "humidity"
    private static final String O2 = "o2"
    private static final String CO2 = "co2"
    private static final String REEFER_TEMP_REQUIRED = "reefer-temp-required"
    private static final String REEFER_VENT_REQUIRED = "reefer-vent-required"
    private static final String IS_WHEELED = "W"
}
