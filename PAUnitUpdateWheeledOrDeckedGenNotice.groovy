import com.navis.argo.business.model.GeneralReference
import com.navis.argo.business.model.LocPosition
import com.navis.external.services.AbstractGeneralNoticeCodeExtension
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.services.business.event.Event
import com.navis.services.business.event.GroovyEvent
import com.navis.xpscache.business.atoms.EquipBasicLengthEnum
import org.apache.log4j.Logger

/*
* @Author:  rgopal@weservetech.com; Date: 06-Aug-2024
*
* Requirements: When Unit placed in Wheeled/Grounded yard block, Then ufvFlexString02 updated as W(Wheeled) or D(Grounded).
*
* @Inclusion Location: Incorporated as a code extension of the type
*
* Load Code Extension to N4:
* 1. Go to Administration --> System --> Code Extensions
* 2. Click Add (+)
* 3. Enter the values as below:
*    Code Extension Name: PAUnitUpdateWheeledOrDeckedGenNotice
*    Code Extension Type: GENERAL_NOTICES_CODE_EXTENSION
*    Groovy Code: Copy and paste the contents of groovy code.
* 4. Click Save button
*
*  * PLEASE UPDATE THE BELOW HISTORY WHENEVER THE GROOVY IS MODIFIED
 * History:
 * {Date}:{Author}:{WorkItem#}:{short issue/solution description}
 * 13-08-2024 : Tharani G : WBCT-194 : When Unit placed in the invalid yard block, then ufvFlexString02 updated as null
*/

class PAUnitUpdateWheeledOrDeckedGenNotice extends AbstractGeneralNoticeCodeExtension {
    private static Logger LOGGER = Logger.getLogger(PAUnitUpdateWheeledOrDeckedGenNotice.class);
    @Override
    void execute(GroovyEvent inGroovyEvent) {
        Event event = inGroovyEvent.getEvent()
        if (event != null) {
            Unit unit = (Unit) inGroovyEvent.getEntity()
            if (unit != null) {
                UnitFacilityVisit unitFacilityVisit = unit.getUnitActiveUfvNowActive()
                if (unitFacilityVisit != null) {
                    LocPosition ufvPosition = (LocPosition) unitFacilityVisit.getUfvLastKnownPosition()
                    if (ufvPosition != null) {
                        boolean isValidPos = true

                        if (ufvPosition.isGrounded()) {
                            unitFacilityVisit.setUfvFlexString02("D")
                        }

                        String abnNameAlt
                        if (EquipBasicLengthEnum.BASIC20.equals(unit.getUnitEquipment().getEqEquipType().getEqtypBasicLength())) {
                            abnNameAlt = ufvPosition?.getPosBin()?.getAbnName()
                        } else {
                            abnNameAlt = ufvPosition?.getPosBin()?.getAbnNameAlt()
                        }
                        if (abnNameAlt == null) {
                            unitFacilityVisit.setUfvFlexString02(null)
                            isValidPos = false
                        }

                        String slot = ufvPosition?.getPosSlot()
                        if (slot != null) {
                            if (slot.contains(".")) {
                                slot = slot.substring(0, slot.lastIndexOf("."));
                            }
                        }
                        if (slot != null && abnNameAlt != null) {
                            if (!slot.equals(abnNameAlt)) {
                                unitFacilityVisit.setUfvFlexString02(null)
                                isValidPos = false
                            }
                        }
                        if (isValidPos) {
                            if (ufvPosition.isWheeled()) {
                                unitFacilityVisit.setUfvFlexString02("W")
                            }
                        }
                        // Groovy validation to identify - Begin
                        LOGGER.debug("PAUnitUpdateWheeledOrDeckedGenNotice - ufvPosition : " + ufvPosition)
                        GeneralReference chinaSideGenRef = GeneralReference.findUniqueEntryById("WBCT","YARD_SIDE_VALIDATION","CHINA_SIDE","BLOCK_LIST")
                        GeneralReference excludedBlocksGenRef = GeneralReference.findUniqueEntryById("WBCT","YARD_SIDE_VALIDATION","EXCLUDED_BLOCKS","BLOCK_LIST")
                        String yardSide = null
                        String ufvBlock = ufvPosition.getBlockName()
                        LOGGER.debug("PAUnitUpdateWheeledOrDeckedGenNotice - ufvBlock : " + ufvBlock)
                        if (chinaSideGenRef != null && ufvBlock != null) {
                            if(chinaSideGenRef.getRefValue1()?.contains(ufvPosition.getBlockName()) || chinaSideGenRef.getRefValue2()?.contains(ufvPosition.getBlockName()) || chinaSideGenRef.getRefValue3()?.contains(ufvPosition.getBlockName()) || chinaSideGenRef.getRefValue4()?.contains(ufvPosition.getBlockName()) || chinaSideGenRef.getRefValue5()?.contains(ufvPosition.getBlockName()) || chinaSideGenRef.getRefValue6()?.contains(ufvPosition.getBlockName())){
                                yardSide = "CHINA"
                            }
                            else if(excludedBlocksGenRef == null || (excludedBlocksGenRef != null && !excludedBlocksGenRef.getRefValue1()?.contains(ufvPosition.getBlockName()) && !excludedBlocksGenRef.getRefValue2()?.contains(ufvPosition.getBlockName()) && !excludedBlocksGenRef.getRefValue3()?.contains(ufvPosition.getBlockName()) && !excludedBlocksGenRef.getRefValue4()?.contains(ufvPosition.getBlockName()) && !excludedBlocksGenRef.getRefValue5()?.contains(ufvPosition.getBlockName()) && !excludedBlocksGenRef.getRefValue6()?.contains(ufvPosition.getBlockName()))){
                                yardSide = "YANGMING"
                            }

                            LOGGER.debug("PAUnitUpdateWheeledOrDeckedGenNotice - yardSide : " + yardSide)

                        }
                        unitFacilityVisit.setUfvFlexString04(yardSide)
                        // Groovy validation to identify - End
                    }
                }
            }
        }
    }
}