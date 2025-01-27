package WBCT

import com.navis.argo.ArgoPropertyKeys
import com.navis.argo.ContextHelper
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.reference.PointCall
import com.navis.argo.business.reference.RoutingPoint
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.argo.util.XmlUtil
import com.navis.external.argo.AbstractArgoCustomWSHandler
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.BizFailure
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageCollector
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.units.Unit
import com.navis.orders.OrdersPropertyKeys
import com.navis.orders.business.eqorders.Booking
import com.navis.orders.business.eqorders.EquipmentOrderItem
import com.navis.road.RoadPropertyKeys
import com.navis.road.business.util.RoadBizUtil
import com.navis.services.business.rules.EventType
import com.navis.vessel.VesselEntity
import com.navis.vessel.business.schedule.VesselVisitDetails
import com.navis.vessel.business.schedule.VesselVisitLine
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.jdom.Element
import org.jdom.Namespace

/**
 * @Author: uaarthi@weservetech.com; Date: 16/01/2023
 *
 *  Requirements: The groovy is used for TWP integration to handle the web service request to update booking POD,Vessel visit roll and booking item quantity.
 *
 * @Inclusion Location: Incorporated as a code extension of the type
 *
 *  Load Code Extension to N4:
 *  1. Go to Administration --> System --> Code Extensions
 *  2. Click Add (+)
 *  3. Enter the values as below:
 *     Code Extension Name: PABatchUpdateBookHandler
 *     Code Extension Type:  WS_ARGO_CUSTOM_HANDLER
 *     Groovy Code: Copy and paste the contents of groovy code.
 *  4. Click Save button
 *
 * Modified - @Author: emonika@weservetech.com; Date: 13/12/2023 - The groovy is used for TWP integration to handle the web service request to update booking POD,Vessel visit roll for only Storage/departed unit.
 *
 */
class PABatchUpdateBookHandler_backup extends AbstractArgoCustomWSHandler {
    private static final Logger logger = Logger.getLogger(PABatchUpdateBookHandler_backup.class)


    @Override
    void execute(UserContext uc, MessageCollector mc, Element inElement, Element inOutElement, Long aLong) {
        logger.setLevel(Level.DEBUG)
        Element rootElement = inElement.getChild("custom-booking")
        Element currentBKGResponse = new Element("bookings")
        List<Element> bookElements = (List<Element>) rootElement.getChildren("booking")
        Element bookItemElements = rootElement.getChild("booking-items")

        if (bookElements != null && !bookElements.isEmpty()) {
            StringBuilder errorBuilder = new StringBuilder()
            VesselVisitDetails vesselVisitDetails = null
            String[] books = null
            Element bookElement = bookElements.get(0)
            String bkgGkey = bookElement.getAttributeValue("gkey")
            String vslVisit = bookElement.getAttributeValue("vessel-visit")
            if (bkgGkey == null || bkgGkey.isEmpty()) {
                mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, null, "Book Gkey")
                errorBuilder.append("VALIDATION_REQUIRED_FIELD_BOOKING")

            } else {
                books = bkgGkey.split(",")*.trim()

                if (books.size() > 50) {
                    getMessageCollector().appendMessage(MessageLevel.INFO, PropertyKeyFactory.valueOf("Update received for Booking beyond limit. Please note that first 50 will be considered."), null, null)
                }
            }


            if (StringUtils.isEmpty(vslVisit)) {
                mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, null, "Vessel Visit")
                errorBuilder.append("VALIDATION_REQUIRED_FIELD_VESSEL_VISIT;")
            } else {
                DomainQuery vesselVisitQuery = QueryUtils.createDomainQuery(VesselEntity.VESSEL_VISIT_DETAILS)
                        .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("cvdCv.cvId"), vslVisit))

                vesselVisitDetails = (VesselVisitDetails) HibernateApi.getInstance().getUniqueEntityByDomainQuery(vesselVisitQuery)

                if (vesselVisitDetails == null) {
                    mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf("VISIT_NOT_FOUND"), "Could not find a vessel visit ${vslVisit} for the vessel and ObVoyage details", vslVisit)
                    errorBuilder.append("VISIT_NOT_FOUND;")
                } else {
                    CarrierVisit carrierVisit = CarrierVisit.findVesselVisit(ContextHelper.getThreadFacility(), vslVisit);
                    if (carrierVisit != null && !carrierVisit.isVisitPhaseActive()) {
                        mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf("VESSEL VISIT NOT ACTIVE"), "Vessel visit ${vslVisit} is already not active", null)
                        errorBuilder.append("Vessel visit ${vslVisit} is not active;")
                        vesselVisitDetails = null;
                    }
                }
            }
            if (vesselVisitDetails != null) {
                if (books != null && books.size() > 0) {
                    for (String bkg : books) {
                        try {
                            Booking booking = Booking.hydrate(bkg)
                            if (booking != null) {
                                boolean vvdLineExists = false
                                boolean hasException = false
                                ScopedBizUnit scopedBizUnit = booking?.getEqoLine()
                                String pod = booking?.getEqoPod1()?.getPointId()
                                if (bookElement?.getAttributeValue("pod") != null) {
                                    pod = bookElement.getAttributeValue("pod")
                                }
                                if (!booking.getEqoLine().equals(vesselVisitDetails.getVvdBizu())) {
                                    Set vvdLineSet = vesselVisitDetails?.getVvdVvlineSet()
                                    for (VesselVisitLine vesselVisitLine : vvdLineSet) {
                                        if (vesselVisitLine.getVvlineBizu().equals(scopedBizUnit)) {
                                            vvdLineExists = true;
                                            break
                                        }
                                    }
                                    if (!vvdLineExists) {
                                        mc.appendMessage(MessageLevel.WARNING, OrdersPropertyKeys.ERRKEY__SELECTED_VESSEL_VISIT_NOT_SHARED_BY_LINE, null, vesselVisitDetails.getCvdCv()?.getCvId(), scopedBizUnit?.getBzuId())
                                        errorBuilder.append("ERRKEY__SELECTED_VESSEL_VISIT_NOT_SHARED_BY_LINE;")
                                        hasException = true
                                    }
                                }
                                List itineraryPoints = vesselVisitDetails?.getCvdItinerary()?.getItinPoints()
                                if (itineraryPoints != null && itineraryPoints.size() > 0) {
                                    Map<String, PointCall> map = new HashMap<String, PointCall>();
                                    for (PointCall pointCall : itineraryPoints) {
                                        map.put(pointCall.getCallPoint()?.getPointId(), pointCall);
                                    }

                                    if (pod == null || pod.isEmpty()) {
                                        mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, "Port of Discharge")
                                        errorBuilder.append("VALIDATION_REQUIRED_FIELD_POD;")
                                        hasException = true
                                    } else {
                                        if (!map.containsKey(pod)) {
                                            mc.appendMessage(MessageLevel.WARNING, RoadPropertyKeys.GATE__POINT_NOT_IN_ITINERARY, null, pod, vslVisit)
                                            errorBuilder.append("GATE__POINT_NOT_IN_ITINERARY;")
                                            hasException = true
                                        }
                                    }
                                }
                                long tallyIn = booking?.getEqoTallyReceive()
                                long tallyOut = booking?.getEqoTally()
                                List<Unit> unitList = getUnitFinder()?.findUnitsForOrder(booking)
                                logger.debug("unitList" + unitList)
                                if (unitList != null) {
                                    for (Unit unit : unitList) {
                                        if (unit != null) {
                                            if (UnitCategoryEnum.EXPORT.equals(unit.getUnitCategory()) || unit.getUnitActiveUfvNowActive() != null) {
                                                mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf("UNIT_ALREADY_RECEIVED_OR_DELIVERED_FOR_BOOKING"), null, booking?.getEqboNbr())
                                                errorBuilder.append("UNIT_ALREADY_RECEIVED_OR_DELIVERED_FOR_BOOKING;")
                                                break;
                                            }
                                        }
                                    }
                                }
                                if (vesselVisitDetails.getCvdCv() != null && !hasException) {
                                    Element bookingElement = new Element("booking")
                                    booking.setEqoVesselVisit(vesselVisitDetails?.getCvdCv())
                                    booking.setEqoPod1(RoutingPoint.findRoutingPoint(pod))
                                    String customEvent = "BKG_VV_CHANGED"
                                    EventType eventType = EventType.findEventType(customEvent)
                                    booking.recordEvent(eventType, null, "Booking vessel visit is changed", null)

                                    XmlUtil.setOptionalAttribute(bookingElement, "gkey", booking.getEqboGkey(), (Namespace) null);
                                    XmlUtil.setOptionalAttribute(bookingElement, "nbr", booking.getEqboNbr(), (Namespace) null);
                                    XmlUtil.setOptionalAttribute(bookingElement, "visit-id", booking.getEqoVesselVisit(), (Namespace) null);
                                    XmlUtil.setOptionalAttribute(bookingElement, "pod", pod != null ? pod : "", (Namespace) null);
                                    XmlUtil.setOptionalAttribute(bookingElement, "status", "PASS", (Namespace) null);
                                    currentBKGResponse.addContent(bookingElement)
                                    continue
                                }
                            } else {
                                mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf("WS_BOOKING_NUMBER_NOT_FOUND"), null)
                                errorBuilder.append("WS_BOOKING_NUMBER_NOT_FOUND;")
                            }
                        } catch (Exception e) {
                            errorBuilder.append("WS_BOOKING_NUMBER_NOT_FOUND;")
                        }
                        Element bookingElement = new Element("booking")
                        XmlUtil.setOptionalAttribute(bookingElement, "gkey", bkg, (Namespace) null);
                        XmlUtil.setOptionalAttribute(bookingElement, "status", "FAIL", (Namespace) null);
                        XmlUtil.setOptionalAttribute(bookingElement, "messages", errorBuilder.toString(), (Namespace) null);
                        currentBKGResponse.addContent(bookingElement)
                    }
                    inOutElement.addContent(currentBKGResponse)
                }
            } else {
                Element bookingElement = new Element("booking")
                XmlUtil.setOptionalAttribute(bookingElement, "status", "FAIL", (Namespace) null);
                XmlUtil.setOptionalAttribute(bookingElement, "messages", errorBuilder.toString(), (Namespace) null);
                currentBKGResponse.addContent(bookingElement)
                inOutElement.addContent(currentBKGResponse)
            }


        } else if (bookItemElements != null) {
            if (bookItemElements != null) {
                List<Element> bkgItemListEle = (List<Element>) bookItemElements.getChildren("booking-item")
                EquipmentOrderItem orderItem = null
                if (bkgItemListEle != null && !bkgItemListEle.isEmpty()) {
                    for (Element bkgItem : bkgItemListEle) {
                        StringBuilder errorBuilder = new StringBuilder()
                        String bkgNbr = ""
                        String bkgItemGkey = bkgItem.getAttributeValue("gkey")
                        String qty = bkgItem.getAttributeValue("qty")
                        if (StringUtils.isEmpty(bkgItemGkey)) {
                            mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, null, "Book Item Gkey")
                            errorBuilder.append("VALIDATION_REQUIRED_FIELD_BOOK_ITEM;")
                        }

                        if (StringUtils.isEmpty(qty)) {
                            mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, null, "Qty")
                            errorBuilder.append("VALIDATION_REQUIRED_FIELD_QTY;")
                        }

                        try {
                            orderItem = EquipmentOrderItem.hydrate(bkgItemGkey)


                            if (orderItem != null) {
                                bkgNbr = orderItem?.getEqboiOrder()?.getEqboNbr()
                                Long finalQty = Long.valueOf(qty)

                                if (finalQty > 0) {
                                    if (orderItem?.getEqoiTallyLimit() != null && orderItem?.getEqoiTallyLimit() > finalQty) {
                                        mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf("ERRKEY__EQOI_TALLY_LIMIT_GREATER_THAN_QTY"), null, orderItem?.getEqoiTallyLimit(), finalQty)
                                        errorBuilder.append("ERRKEY__EQOI_TALLY_LIMIT_GREATER_THAN_QTY;")
                                    } else if (orderItem?.getEqoiReceiveLimit() != null && orderItem.getEqoiReceiveLimit() > finalQty) {
                                        mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf("ERRKEY__EQOI_RECEIVE_LIMIT_GREATER_THAN_QTY"), null, orderItem?.getEqoiReceiveLimit(), finalQty)
                                        errorBuilder.append("ERRKEY__EQOI_RECEIVE_LIMIT_GREATER_THAN_QTY;")
                                    } else if (orderItem.getEqoiTallyReceive() != null && finalQty < orderItem.getEqoiTallyReceive()) {
                                        mc.appendMessage(MessageLevel.WARNING, OrdersPropertyKeys.ERRKEY_QUANTITY_LESSER_THAN_TALLY, null, finalQty, orderItem?.getEqoiTallyReceive())
                                        errorBuilder.append("ERRKEY_QUANTITY_LESSER_THAN_TALLY;")

                                    } else if (orderItem.getEqoiTally() != null && finalQty < orderItem.getEqoiTally()) {
                                        mc.appendMessage(MessageLevel.WARNING, OrdersPropertyKeys.ERRKEY_QUANTITY_LESS_THAN_TALLY_OUT, null, finalQty, orderItem?.getEqoiTally())
                                        errorBuilder.append("ERRKEY_QUANTITY_LESS_THAN_TALLY_OUT;")

                                    } else {
                                        orderItem.setEqoiQty(finalQty)
                                        HibernateApi.getInstance().save(orderItem)
                                        RoadBizUtil.commit()
                                    }
                                }

                            } else {
                                mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf("ITEM_NOT_FOUND"), null)
                                errorBuilder.append("ITEM_NOT_FOUND;")

                            }
                        } catch (Exception e) {
                            if (e instanceof BizFailure) {
                                logger.info("Do nothing.")

                            } else {
                                mc.appendMessage(MessageLevel.WARNING, PropertyKeyFactory.valueOf("ITEM_NOT_FOUND"), null)
                                errorBuilder.append("ITEM_NOT_FOUND;")

                            }
                        }

                        Element orderItemElement = new Element("booking-item")
                        XmlUtil.setOptionalAttribute(orderItemElement, "gkey", bkgItemGkey, (Namespace) null);
                        if (errorBuilder != null && StringUtils.isNotEmpty(errorBuilder.toString())) {
                            XmlUtil.setOptionalAttribute(orderItemElement, "messages", errorBuilder.toString(), (Namespace) null);
                            XmlUtil.setOptionalAttribute(orderItemElement, "status", "FAIL", (Namespace) null);
                        } else {
                            XmlUtil.setOptionalAttribute(orderItemElement, "status", "PASS", (Namespace) null);
                        }
                        XmlUtil.setOptionalAttribute(orderItemElement, "booking-nbr", bkgNbr, (Namespace) null);
                        inOutElement.addContent(orderItemElement)

                    }
                }
            }
        }
    }

    private static UnitFinder getUnitFinder() {
        return (UnitFinder) Roastery.getBean("unitFinder");
    }

}