package WBCT


import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.model.GeneralReference
import com.navis.argo.business.reference.EquipType
import com.navis.external.road.AbstractGateTaskInterceptor
import com.navis.inventory.business.units.EqBaseOrderItem
import com.navis.orders.business.eqorders.EquipmentOrderItem
import com.navis.road.business.appointment.model.GateAppointment
import com.navis.road.business.atoms.TranSubTypeEnum
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.workflow.TransactionAndVisitHolder
import org.apache.log4j.Logger

import java.text.SimpleDateFormat
import java.time.format.DateTimeParseException

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 24-Sep-2024
*
*  Requirements:WBCT-85- Restrict the gate transaction and creation of appointment, if its lock out logic configured in gen ref
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTValidateLineISOTranTypeGateTaskInterceptor
*     Code Extension Type: GATE_TASK_INTERCEPTOR
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTValidateLineISOTranTypeGateTaskInterceptor extends AbstractGateTaskInterceptor {

    @Override
    void execute(TransactionAndVisitHolder inTran) {
        LOGGER.warn("WBCTValidateLineISOTranTypeGateTaskInterceptor executing...")
        String type = null
        TruckTransaction truckTran = inTran.getTran()
        GateAppointment gateAppt = inTran.getAppt()
        LOGGER.warn("gate appt " + gateAppt)
        if (truckTran == null && gateAppt == null) {
            return
        }
        if (truckTran != null || gateAppt != null) {
            String equipTypeId = retreiveEquipType(truckTran.getTranEquipType(), gateAppt)
            String lineId = truckTran?.getTranLineId() != null ? truckTran.getTranLineId() : (gateAppt?.getGapptLineOperator()?.getBzuId() != null ? gateAppt?.getGapptLineOperator()?.getBzuId() : null)
            TranSubTypeEnum tranSubTypeEnum = truckTran?.getTranSubType() != null ? truckTran.getTranSubType() : (gateAppt.getTranSubTypeEnum() != null ? gateAppt.getTranSubTypeEnum() : null)
            if (tranSubTypeEnum != null) {
                if (TranSubTypeEnum.RM.equals(tranSubTypeEnum)) {
                    type = EMPTY_IN
                } else if (TranSubTypeEnum.DM.equals(tranSubTypeEnum)) {
                    type = EMPTY_OUT
                }
            }
            LOGGER.warn("tran type " + type)
            LOGGER.warn("line id " + lineId)
            if (ArgoUtils.isNotEmpty(equipTypeId) && ArgoUtils.isNotEmpty(lineId) && ArgoUtils.isNotEmpty(type)) {
                boolean isRestricted = validateGeneralReference(equipTypeId, lineId, type)
                LOGGER.warn("is restricted "+isRestricted)
                if (isRestricted) {
                    registerError("Line ISO EMPTY container is not allowed.")
                }
            }
        }
    }

    String retreiveEquipType(EquipType tranEquipType, GateAppointment gappt) {
        String equipType = null

        if (tranEquipType != null) {
            equipType = tranEquipType?.getEqtypArchetype()?.getEqtypId()
        }

        if (equipType == null && gappt != null) {
            if (gappt.getGapptCtrEquipType() != null) {
                EquipType ctrEquipType = gappt.getGapptCtrEquipType()
                equipType = ctrEquipType.getEqtypArchetype().getEqtypId()
            } else if (gappt.getGapptOrderItem() != null) {
                EqBaseOrderItem baseOrderItem = gappt.getGapptOrderItem()
                EquipmentOrderItem orderItem = EquipmentOrderItem.resolveEqoiFromEqboi(baseOrderItem)
                if (orderItem != null) {
                    EquipType orderItemEquipType = orderItem.getEqoiSampleEquipType()
                    if (orderItemEquipType != null) {
                        equipType = orderItemEquipType.getEqtypArchetype().getEqtypId()
                    }
                }
            }
        }
        LOGGER.warn("equip type " + equipType)
        return equipType
    }

    boolean validateGeneralReference(String isoType, String lineId, String tranType) {
        if (ArgoUtils.isNotEmpty(isoType) && ArgoUtils.isNotEmpty(lineId) && ArgoUtils.isNotEmpty(tranType)) {
            GeneralReference genRef = GeneralReference.findUniqueEntryById(EMPTY_SHUT_OUT, tranType, lineId, isoType)
            LOGGER.warn("gen ref " + genRef)
            if (genRef != null && ArgoUtils.isNotEmpty(genRef.getRefValue1())) {
                if (YES.equalsIgnoreCase(genRef.getRefValue1())) {
                    if (ArgoUtils.isNotEmpty(genRef.getRefValue2())) {
                        try {
                            Date genRefDate = parseDate(genRef.getRefValue2())
                            LOGGER.warn("gen ref date " + genRefDate)
                            if (ArgoUtils.timeNow() <= genRefDate) {
                                return true
                            } else {
                                return false
                            }
                        } catch (Exception e) {
                            LOGGER.warn("Exception " + e)
                        }
                    }
                    return true
                }
            }
        }
        return false
    }

    private static Date parseDate(String inputDate) {
        LOGGER.warn("inputDate: " + inputDate)
        Date parsedDate = null;
        String[] formats = new String[4]
        formats[0] = FORMAT_DATE_YYYYMMDDHHMMSS
        formats[1] = FORMAT_DATE_YYYYMMDDHHMM
        formats[2] = FORMAT_DATE_YYYYMMDDHH
        formats[3] = FORMAT_DATE_YYYYMMDD
        for (String format : formats) {
            try {
                if (FORMAT_DATE_YYYYMMDD.equals(format)) {
                    Date date1 = new SimpleDateFormat(format).parse(inputDate);
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date1);
                    cal.set(Calendar.HOUR_OF_DAY, 23);
                    cal.set(Calendar.MINUTE, 59);
                    cal.set(Calendar.SECOND, 59);
                    LOGGER.warn("cal " + cal)
                    LOGGER.warn("cal time " + cal.getTime())
                    return cal.getTime();
                } else {
                    SimpleDateFormat sdf = new SimpleDateFormat(format)
                    parsedDate = sdf.parse(inputDate)
                    LOGGER.warn("parsed date " + parsedDate)
                    break;
                }
            } catch (Exception e) {
                LOGGER.warn("DateTimeParseException exception " + e)
            }
        }
        return parsedDate;
    }

    private static final String FORMAT_DATE_YYYYMMDDHHMMSS = "yyyy-MM-dd HH:mm:ss"
    private static final String FORMAT_DATE_YYYYMMDDHHMM = "yyyy-MM-dd HH:mm"
    private static final String FORMAT_DATE_YYYYMMDDHH = "yyyy-MM-dd HH"
    private static final String FORMAT_DATE_YYYYMMDD = "yyyy-MM-dd"
    private static final String EMPTY_SHUT_OUT = "EMPTY_SHUTOUTS"
    private static final String YES = "YES"
    private static final String EMPTY_IN = "EMPTY_IN"
    private static final String EMPTY_OUT = "EMPTY_OUT"
    private static final Logger LOGGER = Logger.getLogger(WBCTValidateLineISOTranTypeGateTaskInterceptor.class)
}
