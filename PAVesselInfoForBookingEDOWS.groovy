import com.navis.argo.ContextHelper
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.external.argo.AbstractGroovyWSCodeExtension
import com.navis.framework.business.Roastery
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.presentation.internationalization.MessageTranslator
import com.navis.framework.util.DateUtil
import com.navis.framework.util.internationalization.TranslationUtils
import com.navis.inventory.InventoryField
import com.navis.inventory.business.imdg.Hazards
import com.navis.orders.business.eqorders.Booking
import com.navis.orders.business.eqorders.EquipmentDeliveryOrder
import com.navis.vessel.business.schedule.VesselVisitDetails
import com.navis.vessel.business.schedule.VesselVisitLine
import groovy.json.JsonOutput
import org.apache.log4j.Logger

/* Copyright 2017 Ports America.  All Rights Reserved.  This code contains the CONFIDENTIAL and PROPRIETARY information of Ports America.
 * TWP Version RLS529 02/28/2017
 * Description: PAVesselInfoForBookingEDOWS retrieves Booking information for the passed Booking Numbers
 * Author: Geeta Desai
 * Date: 01/20/2017
 * Called From: External web service call (Test it through SoapUI).
 * Date: 03/24/2017

 */

class PAVesselInfoForBookingEDOWS extends AbstractGroovyWSCodeExtension {
    /*
     * execute returns a json string representing vessel information for given booking numbers

     * @param  inParams Map of input parameters:
     *             BOOKING_NUMBERS is a CSV string of container numbers
     *
     * @return      JSON formatted object representing Booking/Release/Order for the containers
     *
     *  S.No    Modified Date   Modified By     Jira         Description
     *  1.      12/09/2024     Sangeetha Velu   WBCT-213    Included the genset required validation in the response.
     *
     */
    private static Logger LOGGER = Logger.getLogger(PAVesselInfoForBookingEDOWS.class)

    String execute(Map<String, Object> inParams) {

        TimeZone tz = ContextHelper.getThreadUserTimezone()
        LOGGER.info("Started : PAVesselInfoForBookingEDOWS ")
        log(String.format("Start PAVesselInfoForBookingEDOWS %s", DateUtil.getTodaysDate(tz)))

        // Used to get language resource translation - specifically for the Enums
        MessageTranslator messageTranslator = TranslationUtils.getTranslationContext(getUserContext()).getMessageTranslator()
        // Used for getting terminal time for logging.

        List<Map<String, Object>> arrBookingEDOReturn = new ArrayList<Map<String, Object>>()

        String[] arrBookingEDONumbers = inParams.get("BOOKING_NBR").split(",")
        String orderType = inParams.get("ORDER_TYPE")
        List<Map<String, Object>> arrReturn;
        for (String bookingEDONumber : arrBookingEDONumbers) {
            if (orderType == "BOOK_OR_EDO") {
                arrReturn = GetBookingInfo(bookingEDONumber)
                if (arrReturn.any()) {
                    arrBookingEDOReturn.addAll(arrReturn)
                } else {
                    arrReturn = GetEDOInfo(bookingEDONumber)
                    if (arrReturn.any()) {
                        arrBookingEDOReturn.addAll(arrReturn)
                    }
                }
            } else if (orderType == "EDO_OR_BOOK") {
                arrReturn = GetEDOInfo(bookingEDONumber)
                if (arrReturn.any()) {
                    arrBookingEDOReturn.addAll(arrReturn)
                } else {
                    arrReturn = GetBookingInfo(bookingEDONumber)
                    if (arrReturn.any()) {
                        arrBookingEDOReturn.addAll(arrReturn)
                    }
                }
            } else {
                if (orderType == "BOOK" || orderType == "BOTH" || orderType == null) {
                    arrReturn = GetBookingInfo(bookingEDONumber)
                    if (arrReturn.any()) {
                        arrBookingEDOReturn.addAll(arrReturn)
                    }
                }
                if (orderType == "EDO" || orderType == "BOTH" || orderType == null) {
                    arrReturn = GetEDOInfo(bookingEDONumber)
                    if (arrReturn.any()) {
                        arrBookingEDOReturn.addAll(arrReturn)
                    }
                }
            }
        }
        return JsonOutput.toJson(arrBookingEDOReturn)
    }

    private List<Map<String, Object>> GetBookingInfo(String bookingNumber) {
        List<Map<String, Object>> arrBookingReturn = new ArrayList<Map<String, Object>>();
        DomainQuery dq =
                QueryUtils
                        .createDomainQuery("Booking")
                        .addDqPredicate(PredicateFactory.eq(InventoryField.EQBO_NBR, bookingNumber))


        List bookingList = Roastery.getHibernateApi().findEntitiesByDomainQuery(dq);

        for (Object objBooking : bookingList) {
            Booking booking = objBooking as Booking
            LOGGER.info("Inside : PAVesselInfoForBookingEDOWS GetBookingInfo ")


            if (booking != null) {
                LOGGER.info("Inside : PAVesselInfoForBookingEDOWS GetBookingInfo not null")

                Map<String, Object> bookingMapping = new HashMap<String, Object>();
                InitData(bookingMapping);
                bookingMapping["SUB_TYPE"] = "BOOK";
                bookingMapping["BOOKINGEDO_NBR"] = booking.getEqboNbr();
                // bookingMapping["BOOKING_NBR"] = booking.getEqboNbr();
                String strShipper = booking.getConsigneeAsString();

                bookingMapping["SHIPPER"] = strShipper == null ? "" : strShipper;

                ScopedBizUnit lineOperator = (ScopedBizUnit) booking.getEqoLine();
                bookingMapping["SSCO_CODE"] = lineOperator == null ? "" : lineOperator.bzuId;
                bookingMapping["TALLY"] = booking.getEqoTally();
                bookingMapping["TALLY_RECEIVE"] = booking.getEqoTallyReceive();
                bookingMapping["IS_HAZARDOUS"] = booking.isHazardous();
                bookingMapping["IS_REEFER"] = booking.getEqoHasReefers();
                bookingMapping["IS_OUT_OF_GATE_BOOKING"] = booking.getEqoOod() ? "Y" : "N";
                bookingMapping["IS_OVERRIDE_CUTOFF"] = booking.getEqoOverrideCutoff();
                bookingMapping["UNIQUE_ID"] = booking.getEqboGkey();
                bookingMapping["HAZARD_CODES"] = booking.getBkgHazardCodes();
                bookingMapping["WORST_HAZARD_CLASS"] = "";
                String POL = booking.getEqoPol()?.getPointId();
                bookingMapping["POL"] = POL == null ? "" : POL;
                Hazards haz = booking.getEqoHazards();
                if (haz != null) {
                    bookingMapping["WORST_HAZARD_CLASS"] = haz.getWorstHazardClass();
                }
                // Vessel Visit
                CarrierVisit vesselVisit = booking.getEqoVesselVisit();
                if (vesselVisit != null) {
                    LOGGER.info("Inside : PAVesselInfoForBookingEDOWS vesselVisit not null ")

                    def vvd = VesselVisitDetails.resolveVvdFromCv(vesselVisit);

                    ScopedBizUnit vvLineBizUnit = null;
                    Date cargoCutOff = null;
                    Date reeferCutOff = null;
                    Date hazCutOff = null;
                    Date emptyPickup = null;
                    Date beginReceive = null;
                    Date beginReeferReceive = null;
                    Date beginHazardousReceive = null;

                    try {
                        if (lineOperator.bzuId != null || lineOperator.bzuId.length() > 0) {
                            Set vvdLines = vvd.getVvdVvlineSet();
                            if (vvdLines != null) {

                                for (VesselVisitLine vvdLine in vvdLines) {

                                    vvLineBizUnit = vvdLine.getVvlineBizu();

                                    if (vvLineBizUnit.getBzuId() == lineOperator.bzuId) {
                                        cargoCutOff = vvdLine.getVvlineTimeCargoCutoff();
                                        reeferCutOff = vvdLine.getVvlineTimeReeferCutoff();
                                        hazCutOff = vvdLine.getVvlineTimeHazCutoff();
                                        beginReceive = vvdLine.getVvlineTimeBeginReceive();
                                        beginReeferReceive = vvdLine.getField(MetafieldIdFactory.valueOf("customFlexFields.vvlineCustomDFFBeginReceiveReefer"));
                                        beginHazardousReceive = vvdLine.getField(MetafieldIdFactory.valueOf("customFlexFields.vvlineCustomDFFBeginReceiveHazardous"));
                                        //_logger.debug(String.format("Dates - cargoCutOff: %s, reeferCutOff: %s, hazCutOff: %s, emptyPickup %s, beginReceive %s", cargoCutOff, reeferCutOff, hazCutOff, emptyPickup, beginReceive));
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex) {
                        _logger.debug(String.format("Error Msg - %s", ex.Message));
                    }

                    bookingMapping["VOYAGE"] = vesselVisit.getCarrierIbVoyNbrOrTrainId(); //require
                    bookingMapping["GENERAL_CUTOFF"] = cargoCutOff != null ? cargoCutOff : vvd?.getVvdTimeCargoCutoff();
                    //require
                    bookingMapping["FIRST_GATE_IN"] = beginReceive != null ? beginReceive : vvd?.getVvdTimeBeginReceive();
                    bookingMapping["VESSEL"] = vesselVisit.getCarrierVehicleId();
                    bookingMapping["VESSEL_NAME"] = vesselVisit.getCarrierVehicleName();
                    bookingMapping["CALL_NBR"] = vesselVisit.getCarrierIbVisitCallNbr();
                    bookingMapping["REEFER_CUTOFF"] = reeferCutOff != null ? reeferCutOff : vvd?.getVvdTimeReeferCutoff();
                    bookingMapping["HAZARDOUS_CUTOFF"] = hazCutOff != null ? hazCutOff : vvd?.getVvdTimeHazCutoff();
                    bookingMapping["OUT_VOYAGE"] = vesselVisit.getCarrierObVoyNbrOrTrainId();
                    bookingMapping["ACTUAL_DEPARTURE"] = vesselVisit.getCvATD();

                    //TODO:Call year?
                    //bookingMapping["vvd"] = vvd;
                    bookingMapping["OPERATOR_ID"] = vesselVisit.getCarrierOperatorId();
                    // bookingMapping["VESSEL_VISIT"] = vesselVisit.getInboundCv();
                    bookingMapping["FIRST_GATE_OUT"] = vvd?.getVvdTimeEmptyPickup();

                    //For Begin Receive validation
                    bookingMapping["BEGIN_REEFER"] = beginReeferReceive != null ? beginReeferReceive : vvd?.getVvFlexDate02();
                    bookingMapping["BEGIN_HAZARDOUS"] = beginHazardousReceive != null ? beginHazardousReceive : vvd?.getVvFlexDate03();
                    //Genset required
                    LOGGER.warn("is genset required " + booking.getFieldValue(isGensetRequiredField))
                    bookingMapping["IS_GENSET_REQUIRED"] = booking.getFieldValue(isGensetRequiredField)
                }

                arrBookingReturn.add(bookingMapping);
            }
        }
        return arrBookingReturn;
    }

    private List<Map<String, Object>> GetEDOInfo(String edoNumber) {
        List<Map<String, Object>> arrEDOReturn = new ArrayList<Map<String, Object>>();
        DomainQuery dq =
                QueryUtils
                        .createDomainQuery("EquipmentDeliveryOrder")
                        .addDqPredicate(PredicateFactory.eq(InventoryField.EQBO_NBR, edoNumber));

        List edoList = Roastery.getHibernateApi().findEntitiesByDomainQuery(dq);
        TimeZone tz = ContextHelper.getThreadUserTimezone();
        log(String.format("Start PAVesselInfoForBookingEDOWSGetEDOInfo %s", DateUtil.getTodaysDate(tz)));
        for (Object objEdo : edoList) {
            EquipmentDeliveryOrder edo = objEdo as EquipmentDeliveryOrder;

            if (edo != null) {
                Map<String, Object> edoMapping = new HashMap<String, Object>();
                InitData(edoMapping)
                edoMapping["SUB_TYPE"] = "EDO";

                edoMapping["BOOKINGEDO_NBR"] = edo.getEqboNbr();
                ScopedBizUnit lineOperator = (ScopedBizUnit) edo.getEqoLine();
                edoMapping["SSCO_CODE"] = lineOperator.bzuId;

                Date lastDate = edo.getEqoLatestDate();
                edoMapping["FIRST_GATE_OUT"] = lastDate == null ? "" : lastDate;
                edoMapping["UNIQUE_ID"] = edo.getEqboGkey();
                edoMapping["TALLY"] = edo.getEqoTally();
                edoMapping["TALLY_RECEIVE"] = edo.getEqoTallyReceive();

                log(String.format("Start PAVesselInfoForBookingEDOWSGetEDOInfo Found %s", DateUtil.getTodaysDate(tz)));
                arrEDOReturn.add(edoMapping);
            }
        }
        return arrEDOReturn;

    }

    private void InitData(Map<String, Object> bookingEdoMapping) {
        bookingEdoMapping["FIRST_GATE_OUT"] = ""
        bookingEdoMapping["UNIQUE_ID"] = ""
        bookingEdoMapping["TALLY"] = ""
        bookingEdoMapping["TALLY_RECEIVE"] = ""
        bookingEdoMapping["IS_HAZARDOUS"] = ""
        bookingEdoMapping["IS_REEFER"] = ""
        bookingEdoMapping["IS_OUT_OF_GATE_BOOKING"] = ""
        bookingEdoMapping["IS_OVERRIDE_CUTOFF"] = ""
        bookingEdoMapping["UNIQUE_ID"] = ""
        bookingEdoMapping["VOYAGE"] = ""
        bookingEdoMapping["GENERAL_CUTOFF"] = ""
        bookingEdoMapping["FIRST_GATE_IN"] = ""
        bookingEdoMapping["VESSEL"] = ""
        bookingEdoMapping["VESSEL_NAME"] = ""
        bookingEdoMapping["CALL_NBR"] = ""
        bookingEdoMapping["REEFER_CUTOFF"] = ""
        bookingEdoMapping["HAZARDOUS_CUTOFF"] = ""
        bookingEdoMapping["OUT_VOYAGE"] = "";
        bookingEdoMapping["ACTUAL_DEPARTURE"] = ""
        bookingEdoMapping["HAZARD_CODES"] = ""
        bookingEdoMapping["WORST_HAZARD_CLASS"] = ""
        bookingEdoMapping["OPERATOR_ID"] = ""
        bookingEdoMapping["POL"] = ""
        bookingEdoMapping["BEGIN_REEFER"] = ""
        bookingEdoMapping["BEGIN_HAZARDOUS"] = ""
        bookingEdoMapping["IS_GENSET_REQUIRED"] = ""
    }
    private static final MetafieldId isGensetRequiredField = MetafieldIdFactory.valueOf("bookCustomFlexFields.bkgCustomDFFGensetRequired");

}
