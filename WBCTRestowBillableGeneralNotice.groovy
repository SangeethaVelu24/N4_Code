package WBCT

import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.RestowTypeEnum
import com.navis.argo.business.model.GeneralReference
import com.navis.external.services.AbstractGeneralNoticeCodeExtension
import com.navis.inventory.InvField
import com.navis.inventory.business.units.Unit
import com.navis.services.business.event.Event
import com.navis.services.business.event.EventFieldChange
import com.navis.services.business.event.GroovyEvent
import com.navis.services.business.rules.EventType
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 26-Dec-2024
*
*  Requirements: WBCT - 380- record the custom billable event for the unit against the Restow type
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTRestowBillableGeneralNotice
*     Code Extension Type: GENERAL_NOTICES_CODE_EXTENSION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTRestowBillableGeneralNotice extends AbstractGeneralNoticeCodeExtension {

    @Override
    void execute(GroovyEvent inGroovyEvent) {
        logger.warn("WBCTRestowBillableGeneralNotice executing...")
        Unit unit = (Unit) inGroovyEvent.getEntity()
        Event event = inGroovyEvent.getEvent()
        Map map = new HashMap()
        if (event != null && inGroovyEvent.wasFieldChanged(InvField.UFV_RESTOW_TYPE.getFieldId())
                && inGroovyEvent.wasFieldChanged(InvField.UFV_HANDLING_REASON.getFieldId())) {
            Set<EventFieldChange> fieldChanges = (Set<EventFieldChange>) event.getEvntFieldChanges();
            EventType eventType = EventType.findEventType(UNIT_RESTOW_BILLABLE)
            for (EventFieldChange fieldChange : fieldChanges) {
                if (fieldChange.getNewVal() != null) {
                    String newVal = fieldChange.getNewVal()
                    if (InvField.UFV_HANDLING_REASON.getFieldId().equals(fieldChange.getEvntfcMetafieldId())) {
                        map.put(InvField.UFV_HANDLING_REASON.getFieldId(), newVal)
                    } else if (InvField.UFV_RESTOW_TYPE.getFieldId().equals(fieldChange.getEvntfcMetafieldId())) {
                        map.put(InvField.UFV_RESTOW_TYPE.getFieldId(), newVal)
                    }
                }
            }
            if (map != null && map.size() > 0) {
                if (RestowTypeEnum.RESTOW.getKey().equals(map.get(InvField.UFV_RESTOW_TYPE.getFieldId()))) {
                    String handlingReason = map.get(InvField.UFV_HANDLING_REASON.getFieldId())
                    logger.warn("handling reason " + handlingReason)
                    boolean isRestow = isRestowBillable(handlingReason)
                    logger.warn("isRestow " + isRestow)
                    if (isRestow && eventType != null) {
                        logger.warn("recording custom billable event...")
                        unit.recordEvent(eventType, null, "Recorded through groovy", ArgoUtils.timeNow())
                    }
                }
            }
        }
    }

    boolean isRestowBillable(String reason) {
        if (ArgoUtils.isNotEmpty(reason)) {
            GeneralReference genRef = GeneralReference.findUniqueEntryById(RESTOW, HANDLING_REASON, reason)
            if (genRef != null) {
                String value = genRef.getRefValue1()
                if (ArgoUtils.isNotEmpty(value) && IS_BILLABLE.equals(value)) {
                    return true
                }
            }
        }
        return false
    }

    private static final String IS_BILLABLE = "Y"
    private static final String RESTOW = "RESTOW"
    private static final String HANDLING_REASON = "HANDLING_REASON"
    private static final String UNIT_RESTOW_BILLABLE = "UNIT_RESTOW_BILLABLE"
    private final static Logger logger = Logger.getLogger(WBCTRestowBillableGeneralNotice.class);
}
