package WBCT

import com.navis.argo.ArgoPropertyKeys
import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.reference.PointCall
import com.navis.argo.business.reference.RoutingPoint
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.argo.util.XmlUtil
import com.navis.external.argo.AbstractArgoCustomWSHandler
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.BizFailure
import com.navis.framework.util.BizViolation
import com.navis.framework.util.internationalization.ITranslationContext
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.internationalization.TranslationUtils
import com.navis.framework.util.internationalization.UserMessage
import com.navis.framework.util.message.MessageCollector
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.units.Unit
import com.navis.orders.OrdersPropertyKeys
import com.navis.orders.business.eqorders.Booking
import com.navis.orders.business.eqorders.EquipmentOrderItem
import com.navis.road.RoadPropertyKeys
import com.navis.road.business.util.RoadBizUtil
import com.navis.vessel.VesselEntity
import com.navis.vessel.business.schedule.VesselVisitDetails
import com.navis.vessel.business.schedule.VesselVisitLine
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.jdom.Element
import org.jdom.Namespace

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 18-Dec-2024
*
*  Requirements: WBCT - 353 - The groovy is used for TWP integration to handle the web service request to update booking POD,Vessel visit roll and booking item quantity.
* Also handles the web service request that deletes the booking from N4 system.
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: PABatchUpdateBookHandler
*     Code Extension Type: WS_ARGO_CUSTOM_HANDLER
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class PABatchUpdateBookHandler extends AbstractArgoCustomWSHandler {
    @Override
    void execute(UserContext userContext, MessageCollector mc, Element inElement, Element outElement, Long aLong) {
        logger.warn("PABatchUpdateBookHandler executing...")
        Element customBookingElement = inElement.getChild(CUSTOM_BOOKING)
        Element responseElement = new Element(BOOKINGS)
        boolean hasException = false

        if (customBookingElement != null) {
            VesselVisitDetails vesselVisitDetails = null
            Element bookItemElements = customBookingElement.getChild(BOOKING_ITEM)
            Element bookingsElement = customBookingElement.getChild(BOOKINGS)
            if (bookingsElement != null) {
                logger.warn("bookings element " + bookingsElement)
                List<Element> bookElements = (List<Element>) bookingsElement.getChildren(BOOKING)
                StringBuilder errorBuilder = new StringBuilder()
                if (bookElements != null && bookElements.size() > 0) {
                    for (Element bookElement : bookElements) {
                        logger.warn("book element " + bookElement)
                        String mode = ArgoUtils.isNotEmpty(bookElement.getAttributeValue(MODE)) ? bookElement.getAttributeValue(MODE) : null
                        String bkgGkey = ArgoUtils.isNotEmpty(bookElement.getAttributeValue(GKEY)) ? bookElement.getAttributeValue(GKEY) : null
                        String vslVisit = ArgoUtils.isNotEmpty(bookElement.getAttributeValue(VESSEL_VISIT)) ? bookElement.getAttributeValue(VESSEL_VISIT) : null
                        logger.warn("bkg gkey " + bkgGkey)
                        if (bkgGkey == null || bkgGkey.isEmpty()) {
                            mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, null, "Book Gkey")
                            errorBuilder.append(VALIDATION_REQUIRED_FIELD_BOOKING)
                        }

                        if (UPDATE.equals(mode)) {
                            if (bkgGkey != null) {
                                try {
                                    Element bookingElement = new Element(BOOKING)
                                    Booking booking = Booking.hydrate(bkgGkey)
                                    if (booking != null) {
                                        boolean vvdLineExists = false
                                        ScopedBizUnit scopedBizUnit = booking?.getEqoLine()
                                        String pod = booking?.getEqoPod1()?.getPointId()
                                        if (bookElement?.getAttributeValue(POD) != null) {
                                            pod = bookElement.getAttributeValue(POD)
                                        }
                                        boolean holdPartial = false
                                        boolean lateReceipt = false
                                        boolean earlyReceipt = false
                                        boolean exception = false
                                        if (ArgoUtils.isNotEmpty(bookElement.getAttributeValue(HOLD_PARTIAL))) {
                                            holdPartial = Boolean.valueOf(bookElement.getAttributeValue(HOLD_PARTIAL))
                                        }
                                        if (ArgoUtils.isNotEmpty(bookElement.getAttributeValue(LATE_RECEIPT))) {
                                            lateReceipt = Boolean.valueOf(bookElement.getAttributeValue(LATE_RECEIPT))
                                        }
                                        if (ArgoUtils.isNotEmpty(bookElement.getAttributeValue(EARLY_RECEIPT))) {
                                            earlyReceipt = Boolean.valueOf(bookElement.getAttributeValue(EARLY_RECEIPT))
                                        }

                                        if (ArgoUtils.isEmpty(bookElement.getAttributeValue(HOLD_PARTIAL)) && ArgoUtils.isEmpty(bookElement.getAttributeValue(LATE_RECEIPT))
                                                && ArgoUtils.isEmpty(bookElement.getAttributeValue(EARLY_RECEIPT))) {
                                            exception = true
                                        }
                                        if (!exception) {
                                            booking.setEqoHoldPartials(holdPartial)
                                            booking.setFieldValue(early_receipt, earlyReceipt)
                                            booking.setFieldValue(late_Receipt, lateReceipt)

                                            XmlUtil.setOptionalAttribute(bookingElement, GKEY, booking.getEqboGkey(), (Namespace) null);
                                            XmlUtil.setOptionalAttribute(bookingElement, NBR, booking.getEqboNbr(), (Namespace) null);
                                            XmlUtil.setOptionalAttribute(bookingElement, HOLD_PARTIAL, booking.getEqoHoldPartials(), (Namespace) null);
                                            XmlUtil.setOptionalAttribute(bookingElement, LATE_RECEIPT, booking.getFieldValue(late_Receipt), (Namespace) null);
                                            XmlUtil.setOptionalAttribute(bookingElement, EARLY_RECEIPT, booking.getFieldValue(early_receipt), (Namespace) null);
                                            XmlUtil.setOptionalAttribute(bookingElement, STATUS, PASS, (Namespace) null);
                                            responseElement.addContent(bookingElement)
                                        }

                                        if (ArgoUtils.isEmpty(bookElement.getAttributeValue(HOLD_PARTIAL)) && ArgoUtils.isEmpty(bookElement.getAttributeValue(EARLY_RECEIPT)) && ArgoUtils.isEmpty(bookElement.getAttributeValue(LATE_RECEIPT))) {
                                            if (StringUtils.isEmpty(vslVisit)) {
                                                mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, null, "Vessel Visit")
                                                errorBuilder.append(VALIDATION_REQUIRED_FIELD_VESSEL_VISIT)
                                            } else {
                                                DomainQuery vesselVisitQuery = QueryUtils.createDomainQuery(VesselEntity.VESSEL_VISIT_DETAILS)
                                                        .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("cvdCv.cvId"), vslVisit))

                                                vesselVisitDetails = (VesselVisitDetails) HibernateApi.getInstance().getUniqueEntityByDomainQuery(vesselVisitQuery)

                                                if (vesselVisitDetails == null) {
                                                    mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf(VISIT_NOT_FOUND), "Could not find a vessel visit ${vslVisit} for the vessel and ObVoyage details", vslVisit)
                                                    errorBuilder.append(VISIT_NOT_FOUND)
                                                } else {
                                                    CarrierVisit carrierVisit = CarrierVisit.findVesselVisit(ContextHelper.getThreadFacility(), vslVisit);
                                                    if (carrierVisit != null && !carrierVisit.isVisitPhaseActive()) {
                                                        mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf(VESSEL_VISIT_NOT_ACTIVE), "Vessel visit ${vslVisit} is already not active", null)
                                                        errorBuilder.append("Vessel visit ${vslVisit} is not active;")
                                                        vesselVisitDetails = null;
                                                    }
                                                }
                                            }
                                        }
                                        if (vesselVisitDetails != null) {
                                            if (!booking.getEqoLine().equals(vesselVisitDetails.getVvdBizu())) {
                                                Set<VesselVisitLine> vvdLineSet = (Set<VesselVisitLine>) vesselVisitDetails?.getVvdVvlineSet()
                                                for (VesselVisitLine vesselVisitLine : vvdLineSet) {
                                                    if (vesselVisitLine.getVvlineBizu().equals(scopedBizUnit)) {
                                                        vvdLineExists = true;
                                                        break
                                                    }
                                                }
                                                if (!vvdLineExists) {
                                                    mc.appendMessage(MessageLevel.WARNING, OrdersPropertyKeys.ERRKEY__SELECTED_VESSEL_VISIT_NOT_SHARED_BY_LINE, null, vesselVisitDetails.getCvdCv()?.getCvId(), scopedBizUnit?.getBzuId())
                                                    errorBuilder.append(ERRKEY__SELECTED_VESSEL_VISIT_NOT_SHARED_BY_LINE)
                                                    hasException = true
                                                }
                                            }
                                            List<PointCall> itineraryPoints = (List<PointCall>) vesselVisitDetails?.getCvdItinerary()?.getItinPoints()
                                            if (itineraryPoints != null && itineraryPoints.size() > 0) {
                                                Map<String, PointCall> map = new HashMap<String, PointCall>();
                                                for (PointCall pointCall : itineraryPoints) {
                                                    map.put(pointCall.getCallPoint()?.getPointId(), pointCall);
                                                }
                                                if (pod == null || pod.isEmpty()) {
                                                    mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, "Port of Discharge")
                                                    errorBuilder.append(VALIDATION_REQUIRED_FIELD_POD)
                                                    hasException = true
                                                } else {
                                                    if (!map.containsKey(pod)) {
                                                        mc.appendMessage(MessageLevel.WARNING, RoadPropertyKeys.GATE__POINT_NOT_IN_ITINERARY, null, pod, vslVisit)
                                                        errorBuilder.append(GATE__POINT_NOT_IN_ITINERARY)
                                                        hasException = true
                                                    }
                                                }
                                            }
                                            List<Unit> unitList = (List<Unit>) getUnitFinder()?.findUnitsForOrder(booking)
                                            if (unitList != null) {
                                                for (Unit unit : unitList) {
                                                    if (unit != null) {
                                                        if (UnitCategoryEnum.EXPORT.equals(unit.getUnitCategory()) || unit.getUnitActiveUfvNowActive() != null) {
                                                            mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf(UNIT_ALREADY_RECEIVED_OR_DELIVERED_FOR_BOOKING), null, booking?.getEqboNbr())
                                                            errorBuilder.append(UNIT_ALREADY_RECEIVED_OR_DELIVERED_FOR_BOOKING)
                                                            break;
                                                        }
                                                    }
                                                }
                                            }
                                            if (vesselVisitDetails.getCvdCv() != null && !hasException) {

                                                booking.setEqoVesselVisit(vesselVisitDetails?.getCvdCv())
                                                booking.setEqoPod1(RoutingPoint.findRoutingPoint(pod))
                                                XmlUtil.setOptionalAttribute(bookingElement, GKEY, booking.getEqboGkey(), (Namespace) null);
                                                XmlUtil.setOptionalAttribute(bookingElement, NBR, booking.getEqboNbr(), (Namespace) null);
                                                XmlUtil.setOptionalAttribute(bookingElement, VISIT_ID, booking.getEqoVesselVisit(), (Namespace) null);
                                                XmlUtil.setOptionalAttribute(bookingElement, POD, pod != null ? pod : "", (Namespace) null);
                                                XmlUtil.setOptionalAttribute(bookingElement, STATUS, PASS, (Namespace) null);
                                                responseElement.addContent(bookingElement)
                                                continue
                                            }
                                            if (hasException) {
                                                XmlUtil.setOptionalAttribute(bookingElement, GKEY, bkgGkey, (Namespace) null);
                                                XmlUtil.setOptionalAttribute(bookingElement, STATUS, FAIL, (Namespace) null);
                                                XmlUtil.setOptionalAttribute(bookingElement, MESSAGES, errorBuilder.toString(), (Namespace) null);
                                                responseElement.addContent(bookingElement)
                                            }

                                        }
                                    } else {
                                        mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf(WS_BOOKING_NUMBER_NOT_FOUND), null)
                                        errorBuilder.append(WS_BOOKING_NUMBER_NOT_FOUND)
                                    }
                                } catch (Exception e) {
                                    errorBuilder.append(WS_BOOKING_NUMBER_NOT_FOUND)
                                    Element bookingElement = new Element(BOOKING)
                                    XmlUtil.setOptionalAttribute(bookingElement, GKEY, bkgGkey, (Namespace) null);
                                    XmlUtil.setOptionalAttribute(bookingElement, STATUS, FAIL, (Namespace) null);
                                    XmlUtil.setOptionalAttribute(bookingElement, MESSAGES, errorBuilder.toString(), (Namespace) null);
                                    responseElement.addContent(bookingElement)
                                }
                            } else {
                                Element bookingElement = new Element(BOOKING)
                                XmlUtil.setOptionalAttribute(bookingElement, STATUS, FAIL, (Namespace) null);
                                XmlUtil.setOptionalAttribute(bookingElement, MESSAGES, errorBuilder.toString(), (Namespace) null);
                                responseElement.addContent(bookingElement)
                                outElement.addContent(responseElement)
                            }
                        } else if (DELETE.equals(mode)) {
                            try {
                                logger.warn("delete mode executing...")
                                Booking currentBooking = (Booking) HibernateApi.getInstance().get(Booking.class, Long.parseLong(bkgGkey));
                                Element bookingElement = new Element(BOOKING)
                                XmlUtil.setOptionalAttribute(bookingElement, GKEY, bkgGkey, (Namespace) null);
                                if (currentBooking != null) {
                                    XmlUtil.setOptionalAttribute(bookingElement, NBR, currentBooking.getEqboNbr(), (Namespace) null);
                                    HibernateApi.getInstance().delete(currentBooking)
                                    HibernateApi.getInstance().flush()
                                    XmlUtil.setOptionalAttribute(bookingElement, STATUS, PASS, (Namespace) null);
                                    responseElement.addContent(bookingElement)

                                } else {
                                    throw BizViolation.create(PropertyKeyFactory.valueOf("Booking not found for Gkey: ${bkgGkey}"), null)
                                }
                            } catch (BizViolation | BizFailure bizViolation) {
                                responseElement.setAttribute(STATUS, "3")
                                responseElement.setAttribute(STATUS_ID, bizViolation.getSeverity().getName())
                                Element messages = new Element(MESSAGES)
                                responseElement.addContent(messages)
                                Element message = new Element(MESSAGE)
                                messages.addContent(message)
                                message.setAttribute(GKEY, bkgGkey)
                                message.setAttribute(STATUS, FAIL)
                                String translatedString
                                if (bizViolation instanceof BizViolation) {
                                    BizViolation bv = bizViolation
                                    ITranslationContext translationContext = TranslationUtils.getTranslationContext(ContextHelper.getThreadUserContext());
                                    translatedString = translationContext.getMessageTranslator().getMessage(PropertyKeyFactory.valueOf(bv.getResourceKey()));

                                    if (bv.getParms() != null) {
                                        for (int i = 0; i < bv.getParms().length; ++i) {
                                            Object param = bv.getParms()[i];
                                            if (param != null) {
                                                translatedString = StringUtils.replace(translatedString, "{" + i + "}", param.toString());
                                            }
                                        }
                                    }
                                } else if (bizViolation instanceof BizFailure) {
                                    BizFailure bv = bizViolation

                                    Throwable t = bv.getCause()
                                    if (t != null) {
                                        String bizViolationMessage = t.getMessage()
                                        int keyIndex = bizViolationMessage.indexOf("key=");
                                        int paramsIndex = bizViolationMessage.indexOf("parms=[");

                                        String key = bizViolationMessage.substring(keyIndex + 4, paramsIndex);
                                        String params = bizViolationMessage.substring(paramsIndex + 7, bizViolationMessage.lastIndexOf("]"));
                                        ITranslationContext translationContext = TranslationUtils.getTranslationContext(ContextHelper.getThreadUserContext());
                                        translatedString = translationContext.getMessageTranslator().getMessage(PropertyKeyFactory.valueOf(key.trim()));
                                        if (params != null && !params.equalsIgnoreCase("null")) {
                                            String[] paramsArray = params.split(",")
                                            for (int i = 0; i < paramsArray.length; ++i) {
                                                Object param = paramsArray[i];
                                                if (param != null) {
                                                    translatedString = StringUtils.replace(translatedString, "{" + i + "}", param.toString());
                                                }
                                            }

                                        }

                                    }

                                }
                                if (translatedString != null) {
                                    message.setAttribute(MESSAGE_TEXT, translatedString)
                                    hasException = true
                                }
                                for (UserMessage userMessage : RoadBizUtil.getMessageCollector().getMessages()) {
                                    hasException = true
                                    message = new Element(MESSAGE)
                                    messages.addContent(message)
                                    message.setAttribute(MESSAGE_TEXT, userMessage.toString())
                                    RoadBizUtil.removeMessageFromMessageCollector(userMessage.getMessageKey())
                                }
                            }

                        }
                    }
                }
            } else if (bookItemElements != null) {
                if (bookItemElements != null) {
                    List<Element> bkgItemListEle = (List<Element>) bookItemElements.getChildren(BOOKING_ITEM)
                    EquipmentOrderItem orderItem = null
                    if (bkgItemListEle != null && !bkgItemListEle.isEmpty()) {
                        for (Element bkgItem : bkgItemListEle) {
                            StringBuilder errorBuilder = new StringBuilder()
                            String bkgNbr = ""
                            String bkgItemGkey = bkgItem.getAttributeValue(GKEY)
                            String qty = bkgItem.getAttributeValue(QTY)
                            if (StringUtils.isEmpty(bkgItemGkey)) {
                                mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, null, "Book Item Gkey")
                                errorBuilder.append(VALIDATION_REQUIRED_FIELD_BOOK_ITEM)
                            }

                            if (StringUtils.isEmpty(qty)) {
                                mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, null, "Qty")
                                errorBuilder.append(VALIDATION_REQUIRED_FIELD_QTY)
                            }

                            try {
                                orderItem = EquipmentOrderItem.hydrate(bkgItemGkey)


                                if (orderItem != null) {
                                    bkgNbr = orderItem?.getEqboiOrder()?.getEqboNbr()
                                    Long finalQty = Long.valueOf(qty)

                                    if (finalQty > 0) {
                                        if (orderItem?.getEqoiTallyLimit() != null && orderItem?.getEqoiTallyLimit() > finalQty) {
                                            mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf(ERRKEY__EQOI_TALLY_LIMIT_GREATER_THAN_QTY), null, orderItem?.getEqoiTallyLimit(), finalQty)
                                            errorBuilder.append(ERRKEY__EQOI_TALLY_LIMIT_GREATER_THAN_QTY)
                                        } else if (orderItem?.getEqoiReceiveLimit() != null && orderItem.getEqoiReceiveLimit() > finalQty) {
                                            mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf(ERRKEY__EQOI_RECEIVE_LIMIT_GREATER_THAN_QTY), null, orderItem?.getEqoiReceiveLimit(), finalQty)
                                            errorBuilder.append(ERRKEY__EQOI_RECEIVE_LIMIT_GREATER_THAN_QTY)
                                        } else if (orderItem.getEqoiTallyReceive() != null && finalQty < orderItem.getEqoiTallyReceive()) {
                                            mc.appendMessage(MessageLevel.WARNING, OrdersPropertyKeys.ERRKEY_QUANTITY_LESSER_THAN_TALLY, null, finalQty, orderItem?.getEqoiTallyReceive())
                                            errorBuilder.append(ERRKEY_QUANTITY_LESSER_THAN_TALLY)

                                        } else if (orderItem.getEqoiTally() != null && finalQty < orderItem.getEqoiTally()) {
                                            mc.appendMessage(MessageLevel.WARNING, OrdersPropertyKeys.ERRKEY_QUANTITY_LESS_THAN_TALLY_OUT, null, finalQty, orderItem?.getEqoiTally())
                                            errorBuilder.append(ERRKEY_QUANTITY_LESS_THAN_TALLY_OUT)

                                        } else {
                                            orderItem.setEqoiQty(finalQty)
                                            HibernateApi.getInstance().save(orderItem)
                                            RoadBizUtil.commit()
                                        }
                                    }

                                } else {
                                    mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf(ITEM_NOT_FOUND), null)
                                    errorBuilder.append(ITEM_NOT_FOUND)

                                }
                            } catch (Exception e) {
                                if (e instanceof BizFailure) {
                                    logger.info("Do nothing.")

                                } else {
                                    mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf(ITEM_NOT_FOUND), null)
                                    errorBuilder.append(ITEM_NOT_FOUND)

                                }
                            }

                            Element orderItemElement = new Element(BOOKING_ITEM)
                            XmlUtil.setOptionalAttribute(orderItemElement, GKEY, bkgItemGkey, (Namespace) null);
                            if (errorBuilder != null && StringUtils.isNotEmpty(errorBuilder.toString())) {
                                XmlUtil.setOptionalAttribute(orderItemElement, MESSAGES, errorBuilder.toString(), (Namespace) null);
                                XmlUtil.setOptionalAttribute(orderItemElement, STATUS, FAIL, (Namespace) null);
                            } else {
                                XmlUtil.setOptionalAttribute(orderItemElement, STATUS, PASS, (Namespace) null);
                            }
                            XmlUtil.setOptionalAttribute(orderItemElement, "" + BOOKING_NBR, bkgNbr, (Namespace) null);
                            outElement.addContent(orderItemElement)

                        }
                    }
                }
            }

            outElement.addContent(responseElement)
        }
    }

    private static UnitFinder getUnitFinder() {
        return (UnitFinder) Roastery.getBean("unitFinder");
    }
    private static final String CUSTOM_BOOKING = "custom-booking"
    private static final String BOOKINGS = "bookings"
    private static final String BOOKING = "booking"
    private static final String GKEY = "gkey"
    private static final String VESSEL_VISIT = "vessel-visit"
    private static final String MODE = "mode"
    private static final String UPDATE = "update"
    private static final String DELETE = "delete"
    private static final String ITEM_NOT_FOUND = "ITEM_NOT_FOUND"
    private static final String ERRKEY_QUANTITY_LESSER_THAN_TALLY = "ERRKEY_QUANTITY_LESSER_THAN_TALLY"
    private static final String ERRKEY_QUANTITY_LESS_THAN_TALLY_OUT = "ERRKEY_QUANTITY_LESS_THAN_TALLY_OUT"
    private static final String ERRKEY__EQOI_RECEIVE_LIMIT_GREATER_THAN_QTY = "ERRKEY__EQOI_RECEIVE_LIMIT_GREATER_THAN_QTY"
    private static final String ERRKEY__EQOI_TALLY_LIMIT_GREATER_THAN_QTY = "ERRKEY__EQOI_TALLY_LIMIT_GREATER_THAN_QTY"
    private static final String WS_BOOKING_NUMBER_NOT_FOUND = "WS_BOOKING_NUMBER_NOT_FOUND"
    private static final String VALIDATION_REQUIRED_FIELD_QTY = "VALIDATION_REQUIRED_FIELD_QTY"
    private static final String VALIDATION_REQUIRED_FIELD_BOOK_ITEM = "VALIDATION_REQUIRED_FIELD_BOOK_ITEM"
    private static final String ERRKEY__SELECTED_VESSEL_VISIT_NOT_SHARED_BY_LINE = "ERRKEY__SELECTED_VESSEL_VISIT_NOT_SHARED_BY_LINE"
    private static final String UNIT_ALREADY_RECEIVED_OR_DELIVERED_FOR_BOOKING = "UNIT_ALREADY_RECEIVED_OR_DELIVERED_FOR_BOOKING"
    private static final String GATE__POINT_NOT_IN_ITINERARY = "GATE__POINT_NOT_IN_ITINERARY"
    private static final String VALIDATION_REQUIRED_FIELD_POD = "VALIDATION_REQUIRED_FIELD_POD"
    private static final String VALIDATION_REQUIRED_FIELD_BOOKING = "VALIDATION_REQUIRED_FIELD_BOOKING"
    private static final String VISIT_NOT_FOUND = "VISIT_NOT_FOUND"
    private static final String VESSEL_VISIT_NOT_ACTIVE = "VESSEL VISIT NOT ACTIVE"
    private static final String VALIDATION_REQUIRED_FIELD_VESSEL_VISIT = "VALIDATION_REQUIRED_FIELD_VESSEL_VISIT"
    private static final String FAIL = "FAIL"
    private static final String PASS = "PASS"
    private static final String BOOKING_ITEMS = "booking-items"
    private static final String BOOKING_ITEM = "booking-item"
    private static final String STATUS = "status"
    private static final String STATUS_ID = "status-id"
    private static final String MESSAGES = "messages"
    private static final String MESSAGE = "message"
    private static final String MESSAGE_TEXT = "message-text"
    private static final String BOOKING_NBR = "booking-nbr"
    private static final String QTY = "qty"
    private static final String NBR = "nbr"
    private static final String VISIT_ID = "visit-id"
    private static final String POD = "pod"
    private static final String HOLD_PARTIAL = "hold-partial"
    private static final String LATE_RECEIPT = "late-receipt"
    private static final String EARLY_RECEIPT = "early-receipt"
    private static final MetafieldId early_receipt = MetafieldIdFactory.valueOf("bookCustomFlexFields.bkgCustomDFFEarlyFullReceipt");
    private static final MetafieldId late_Receipt = MetafieldIdFactory.valueOf("bookCustomFlexFields.bkgCustomDFFLateFullReceipt");

    private static final Logger logger = Logger.getLogger(PABatchUpdateBookHandler.class)
}
