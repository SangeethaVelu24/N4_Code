package WBCT

import com.navis.argo.ContextHelper
import com.navis.argo.business.model.LocPosition
import com.navis.argo.business.reference.Container
import com.navis.argo.business.reference.Equipment
import com.navis.framework.business.Roastery
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit

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

class WBCTUpdateWheeledOrDeckedScriptRunner {

    String execute() {
        StringBuilder sb = new StringBuilder()

        String equipId = "TESU1472581"
        /*    Equipment equip = Equipment.findEquipment(equipId)
            if (equip != null) {
                UnitFinder unitFinder = (UnitFinder) Roastery.getBean(UnitFinder.BEAN_ID)
                Unit unit = unitFinder.findActiveUnit(ContextHelper.threadComplex, equip)
                sb.append("unit " + unit).append("\n")
                if (unit) {
                    UnitFacilityVisit unitFacilityVisit = unit.getUnitActiveUfvNowActive()
                    sb.append("ufv " + unitFacilityVisit).append("\n")
                    if (unitFacilityVisit != null) {
                        LocPosition ufvPosition = (LocPosition) unitFacilityVisit.getUfvLastKnownPosition()
                        sb.append("ufv position " + ufvPosition).append("\n")
                        sb.append("is grounded " + ufvPosition.isGrounded()).append("\n")
                        sb.append("is wheeled "+ufvPosition.isWheeled()).append("\n")
                    }
                }
            }*/
        sb.append("ctr nbr " + equipId).append("\n")
        Container container = Container.findContainerByFullId(equipId)
        sb.append("findContainerByFullId " + Container.findContainerByFullId(equipId)).append("\n")

        sb.append("findContainerWithoutValidation " + Container.findContainerWithoutValidation(equipId)).append("\n")
        sb.append("findFullIdOrPadCheckDigit "+Container.findFullIdOrPadCheckDigit(equipId)).append("\n")
        sb.append("isContainerIdValid "+Container.isContainerIdValid(equipId)).append("\n")

        return sb
    }
}
