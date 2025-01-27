import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ServicesManager
import com.navis.argo.business.atoms.EquipIsoGroupEnum
import com.navis.argo.business.reference.Container
import com.navis.argo.business.reference.EquipType
import com.navis.external.argo.AbstractGroovyWSCodeExtension
import com.navis.framework.business.Roastery
import com.navis.framework.presentation.internationalization.MessageTranslator
import com.navis.framework.util.DateUtil
import com.navis.framework.util.internationalization.TranslationUtils
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.units.EquipmentState
import com.navis.inventory.business.units.Unit
import groovy.json.JsonOutput

/* Copyright 2017 Ports America.  All Rights Reserved.  This code contains the CONFIDENTIAL and PROPRIETARY information of Ports America.

 * Description: PAGetSizeTypeForContainersWS retrieves container  information based on passed container number. It will retrive length from
 * and height from conrainer . Type from equipment Equivalents.
 * Author: Geeta Desai
 * Date: 06/19/2017
 * Called From: External web service call (Test it through argoTester).
 * Mod. on 11/9/2017 By Geeta to get build date for Fleet file
 *  * Mod. on 11/16/2017 By Geeta to get operator id as owner id
 * Mod. on 06/06/2022 By Geeta- to remove equipment type Equivalents
 * 03/29/2023 By Young to reset by container number in TOS to handle container number with different check digit.
 */

public class PAGetSizeTypeForContainersWS extends AbstractGroovyWSCodeExtension {

    public String execute(Map<String, Object> inParams) {
        TimeZone tz = ContextHelper.getThreadUserTimezone();
        log(String.format("Start PAGetSizeTypeForContainersWSWS %s", DateUtil.getTodaysDate(tz)));
        MessageTranslator messageTranslator = TranslationUtils.getTranslationContext(getUserContext()).getMessageTranslator();
        // Used to get language resource translation - specifically for the Enums

        List<Map<String, Object>> arrEquipmentTypes = new ArrayList<Map<String, Object>>();

        String[] containers = inParams.get("CONTAINER_NUMBERS").split(",");
        if (containers == null || containers.length < 1)
        {
            Map<String, Object> error = new HashMap<String, Object>();
            error["ERROR"] = "No containers passed";
            arrEquipmentTypes.add(error);
            return JsonOutput.toJson(arrEquipmentTypes);

        };
        boolean includeUnknown = inParams.get("INCLUDE_UNKNOWN") == "Y";
        for (String container : containers)
        {
            Map<String, Object> containerMapping = new HashMap<String, Object>();

            Container objContainer = Container.findContainer(container);
            containerMapping["EQUIPMENT_NO"] = container;
            if (objContainer == null) {
                if(includeUnknown){
                    containerMapping["KNOWN"] = false;
                    arrEquipmentTypes.add(containerMapping);
                }
                continue;
            }

            containerMapping["EQUIPMENT_NO"] = objContainer.eqIdFull;  // handle if container exists with different check digit or same checkdigit. N4 still return container info
            containerMapping["OWNER_ID"] = objContainer.getEquipmentOperatorId();
            containerMapping["FF_DATE"]  = objContainer.getEqBuildDate();
            EquipType eqType =  objContainer.getEqEquipType();
            containerMapping["ISO_TYPE"] = eqType.getEqtypId();
            EquipIsoGroupEnum groupEnum =   eqType.getEqtypIsoGroup();
            String clength =  messageTranslator.getMessage(eqType.getEqtypNominalLength()?.getDescriptionPropertyKey());
            String height = messageTranslator.getMessage(eqType.getEqtypNominalHeight()?.getDescriptionPropertyKey());

            String isoGroup =  groupEnum.getKey();
            containerMapping["LENGTH"] = messageTranslator.getMessage(eqType.getEqtypNominalLength()?.getDescriptionPropertyKey());
            containerMapping["HEIGHT"] =messageTranslator.getMessage(eqType.getEqtypNominalHeight()?.getDescriptionPropertyKey());
            containerMapping["ISO Height"] = messageTranslator.getMessage(eqType.getEqtypNominalHeight()?.getDescriptionPropertyKey());

            containerMapping["ISO_GROUP"] = messageTranslator.getMessage(groupEnum?.getDescriptionPropertyKey());
            containerMapping["EQ_TYPE"] = "Container";
            containerMapping["TARE_WEIGHT"] = eqType.getTareWeightKg();

            containerMapping["GROUP_ID"] = isoGroup == null? "": isoGroup;
            containerMapping["KNOWN"] = true;
            UnitFinder unitFinder = Roastery.getBean(UnitFinder.BEAN_ID);
            Unit unit = unitFinder.findActiveUnit(ContextHelper.getThreadComplex(), objContainer)
            if (unit != null) {
                containerMapping["INVENTORY_STATUS"] ="1";
            }else{
                containerMapping["INVENTORY_STATUS"] ="";
            }
            EquipmentState equipmentState = EquipmentState.findEquipmentState(objContainer, ContextHelper.getThreadOperator());
            if (equipmentState != null) {
                ServicesManager serviceManager = Roastery.getBean(ServicesManager.BEAN_ID);
                String[] activeHolds = serviceManager.getActiveFlagIds(equipmentState);
                if (activeHolds != null && activeHolds.toString().contains("NO MTY RETURN")) {
                    containerMapping["SERVICE_CODE"] ="15"
                }else{
                    containerMapping["SERVICE_CODE"] =""
                }
            }
            arrEquipmentTypes.add(containerMapping);
        }

        log(String.format("End PAGetSizeTypeForContainersWS %s", DateUtil.getTodaysDate(tz)));
        return JsonOutput.toJson(arrEquipmentTypes);
    }
}

