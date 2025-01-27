import com.navis.argo.ArgoRefField
import com.navis.argo.ContextHelper
import com.navis.argo.business.atoms.EquipClassEnum
import com.navis.argo.business.atoms.EquipRfrTypeEnum
import com.navis.argo.business.reference.EquipType
import com.navis.external.argo.AbstractGroovyWSCodeExtension
import com.navis.framework.business.atoms.LifeCycleStateEnum
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.presentation.internationalization.MessageTranslator
import com.navis.framework.util.DateUtil
import com.navis.framework.util.internationalization.PropertyKey
import com.navis.framework.util.internationalization.TranslationUtils
import com.navis.orders.business.eqorders.EqTypeGroup
import groovy.json.JsonOutput

/* Copyright 2017 Ports America.  All Rights Reserved.  This code contains the CONFIDENTIAL and PROPRIETARY information of Ports America.

 * Description: PAGetContainerTypesWS retrieves size type information for container class.
 * Retrives information from equipment types.
 * Author : Gopal, 08/29/24
 */

public class PAGetContainerTypesWS extends AbstractGroovyWSCodeExtension {
    public String execute(Map<String, Object> inParams) {
        TimeZone tz = ContextHelper.getThreadUserTimezone();
        log(String.format("Start PAGetContainerTypesWS %s", DateUtil.getTodaysDate(tz)));

        // Used to get language resource translation - specifically for the Enums
        MessageTranslator messageTranslator = TranslationUtils.getTranslationContext(getUserContext()).getMessageTranslator();
        List<Map<String, Object>> arrEquipmentTypes = new ArrayList<Map<String, Object>>();

        String parmIncludeDetail = inParams.get("INCLUDE_DETAIL");
        log(String.format("Start PAGetContainerTypesWS INCLUDE_DETAIL %s", parmIncludeDetail ));

        DomainQuery dq = QueryUtils.createDomainQuery("EquipType")
                .addDqPredicate(PredicateFactory.eq( ArgoRefField.EQTYP_LIFE_CYCLE_STATE , LifeCycleStateEnum.ACTIVE.getKey()))
                .addDqPredicate(PredicateFactory.eq(ArgoRefField.EQTYP_IS_ARCHETYPE, Boolean.TRUE))
                .addDqPredicate(PredicateFactory.eq(ArgoRefField.EQTYP_CLASS, EquipClassEnum.CONTAINER.getKey()));

        List<EqTypeGroup> equipmentTypes = HibernateApi.getInstance().findEntitiesByDomainQuery(dq);

        //Equipment type information
        for (EquipType eqType : equipmentTypes) {
            Map<String, Object> eqTypeMapping = new HashMap<String, Object>();
            eqTypeMapping["GKEY"] = eqType.getEqtypGkey();
            eqTypeMapping["ID"] = eqType.getEqtypId();

            PropertyKey pkEquHeight = eqType.getEqtypNominalHeight()?.getDescriptionPropertyKey();
            String strEquHeight = pkEquHeight == null ? "" : messageTranslator.getMessage(pkEquHeight);

            PropertyKey pkLength = eqType.getEqtypNominalLength()?.getDescriptionPropertyKey();
            String strLength = pkLength == null ? "" : messageTranslator.getMessage(pkLength);

            PropertyKey pkISOGroup  = eqType.getEqtypIsoGroup()?.getDescriptionPropertyKey();
            String isoGroup =  pkISOGroup == null ? "" : messageTranslator.getMessage(pkISOGroup);
            eqTypeMapping["HEIGHT"] = strEquHeight == null ? "" : strEquHeight;
            eqTypeMapping["LENGTH"] = strLength == null ? "" : strLength;
            eqTypeMapping["ISO_GROUP"] = isoGroup == null ? "" : isoGroup;

            if (parmIncludeDetail == "Y") {
                eqTypeMapping["DESCRIPTION"] = eqType.getEqtypDescription();
                EquipRfrTypeEnum rfrType =  eqType.getEqtypRfrType();
                eqTypeMapping["REEFER_TYPE"] = rfrType == null? "":   messageTranslator.getMessage(rfrType.getDescriptionPropertyKey())  ;
                eqTypeMapping["IS_TEMPERATURE_CONTROLLED"] = eqType.getEqtypIsTemperatureControlled();
                eqTypeMapping["IS_OOG_OK"] = eqType.getEqtypOogOk();
                eqTypeMapping["USE_ACCESSORIES"] = eqType.usesAccessories();
            }
            arrEquipmentTypes.add(eqTypeMapping);
        }
        log(String.format("End PAGetContainerTypesWS %s", DateUtil.getTodaysDate(tz)));
        return JsonOutput.toJson(arrEquipmentTypes);
    }
}
