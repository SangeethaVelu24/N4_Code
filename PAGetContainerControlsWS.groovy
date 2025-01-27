package WBCT

import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.api.IImpediment
import com.navis.argo.business.api.ServicesManager
import com.navis.argo.business.atoms.FlagStatusEnum
import com.navis.argo.business.reference.Container
import com.navis.argo.business.reference.Equipment
import com.navis.argo.business.reference.LineOperator
import com.navis.external.argo.AbstractGroovyWSCodeExtension
import com.navis.framework.business.Roastery
import com.navis.inventory.business.units.EquipmentState
import com.navis.services.business.rules.FlagType
import groovy.json.JsonOutput
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 30-Sep-2024
*
*  Requirements: WBCT-215 - Retrieves the redeliver no, return location from the equipment state and return as json
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: PAGetContainerControlsWS
*     Code Extension Type: GROOVY_WS_CODE_EXTENSION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class PAGetContainerControlsWS extends AbstractGroovyWSCodeExtension {

    @Override
    String execute(Map<String, String> inParameters) {
        LOGGER.warn("PAGetContainerControlsWS executing...")
        String paramCntrNumber = inParameters.get("PARM_CONTAINER_NUMBERS")
        String paramLineId = inParameters.get("PARM_LINE_ID")
        List<Map<String, Object>> containerList = new ArrayList<Map<String, Object>>();

        if (ArgoUtils.isNotEmpty(paramCntrNumber) && ArgoUtils.isNotEmpty(paramLineId)) {
            String[] cntrArray = paramCntrNumber.split(",")*.trim()
            String[] lineIdArray = paramLineId.split(",")*.trim()
            Set<Equipment> equipSet = new HashSet<>()
            for (String container : cntrArray) {
                for (String lineOp : lineIdArray) {
                    Equipment equip = Container.findEquipment(container)
                    LineOperator line = LineOperator.findLineOperatorById(lineOp)
                    if (equip != null && line != null) {
                        if (line.getBzuId().equalsIgnoreCase(equip.getEquipmentOperatorId())) {
                            equipSet.add(equip)
                            break
                        }
                    }
                }
            }

            if (equipSet != null && equipSet.size() > 0) {
                for (Equipment eq : equipSet) {
                    EquipmentState eqs = EquipmentState.findEquipmentState(eq, ContextHelper.getThreadOperator())
                    boolean hasEmptyReturn = hasEmptyReturnHold(eqs)
                    if (eqs != null && hasEmptyReturn) {
                        Map<String, Object> map = new HashMap<String, Object>();
                        map.put(CNTR_NO, eq.getEqIdFull())
                        map.put(LINE_ID, eq.getEquipmentOperatorId())
                        map.put(DEPOT_NAME, eqs.eqsFlexString02 != null ? eqs.getEqsFlexString02() : "")
                        map.put(REDELIVERY_NO, eqs.eqsFlexString01 != null ? eqs.getEqsFlexString01() : "")
                        map.put(VT_INSTRUCTION, "")
                        containerList.add(map)
                    }
                }
            }
        }
        return JsonOutput.toJson(containerList)
    }

    boolean hasEmptyReturnHold(EquipmentState eq) {
        if (eq!=null){
            ServicesManager servicesManager = Roastery.getBean(ServicesManager.BEAN_ID)
            Collection<IImpediment> impedimentsCollection = (Collection<IImpediment>) servicesManager.getImpedimentsForEntity(eq)
            FlagType flagType = FlagType.findFlagType(NO_EMPTY_RETURN)
            LOGGER.warn("flg type "+flagType)
            LOGGER.warn("flag id "+flagType.getId())
            for (IImpediment iImpediment : impedimentsCollection) {
                LOGGER.warn("impediment "+iImpediment)
                if (iImpediment != null && FlagStatusEnum.ACTIVE.equals(iImpediment.getStatus())) {
                    if (iImpediment?.getFlagType()?.getId() != null && flagType != null && flagType.getId().equals(iImpediment.getFlagType().getId())) {
                        return true
                    }
                }
            }
        }
        return false
    }

    private static final String CNTR_NO = "CNTR_NO"
    private static final String LINE_ID = "LINE_ID"
    private static final String DEPOT_NAME = "DEPOT_NAME"
    private static final String REDELIVERY_NO = "REDELIVERY_NO"
    private static final String VT_INSTRUCTION = "VT_INSTRUCTION"
    private static final String NO_EMPTY_RETURN = "NO MTY RETURN"
    private static final Logger LOGGER = Logger.getLogger(PAGetContainerControlsWS.class);
}
