package WBCT

import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.reference.Container
import com.navis.argo.business.reference.Equipment
import com.navis.argo.business.reference.LineOperator
import com.navis.inventory.business.units.EquipmentState

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

class PAGetContainerControlScriptRunner {

    String execute() {

        StringBuilder sb = new StringBuilder()

        String containers = "CAIU9842362, TEMU8798401, CXRU1452504, SEGU9845420, XYLU8211984"
        String lineOps = "MSC, HMM, YML"
        sb.append("containers " + containers).append("\n")
        sb.append("lineops " + lineOps).append("\n")
        List<Map<String, Object>> containerList = new ArrayList<Map<String, Object>>();

        if (ArgoUtils.isNotEmpty(containers) && ArgoUtils.isNotEmpty(lineOps)) {
            String[] containersArray = containers.split(",")
            String[] lineOpArray = lineOps.split(",")*.trim()
            List<Equipment> equipList = new ArrayList<>()
            sb.append("containers array " + containersArray).append("\n")
            sb.append("line op array " + lineOpArray).append("\n")
            for (String container : containersArray) {
                for (String lineOp : lineOpArray) {
                    Equipment equip = Container.findEquipment(container)
                    LineOperator line = LineOperator.findLineOperatorById(lineOp)
                    Map<String, Equipment> equipmentMap = new HashMap<>()
                    if (equip != null && line != null) {
                        if (line.getBzuId().equalsIgnoreCase(equip.getEquipmentOperatorId())) {
                            equipList.add(equip)
                            // equipmentMap.put("CNTR_NO", equip)
                        }
                    }
                    //sb.append("container map " + equipmentMap).append("\n")
                }
            }
            sb.append("container list " + equipList).append("\n")

            if (equipList != null && equipList.size() > 0) {
                for (Equipment eq : equipList) {
                    EquipmentState eqs = EquipmentState.findEquipmentState(eq, ContextHelper.getThreadOperator())
                    if (eqs != null) {
                        Map<String, Object> map = new HashMap<String, Object>();
                        map.put("CNTR_NO", eq.getEqIdFull())
                        map.put("LINE_ID", eq.getEquipmentOperatorId())
                        map.put("DEPOT_NAME", eqs.eqsFlexString02 != null ? eqs.getEqsFlexString02() : "")
                        map.put("REDELIVERY_NO", eqs.eqsFlexString01 != null ? eqs.getEqsFlexString01() : "")
                        map.put("VT_INSTRUCTION", "")
                        containerList.add(map)
                    }
                }
                sb.append("container list json " + containerList).append("\n")
            }
        }
        return sb.toString()
    }
}
