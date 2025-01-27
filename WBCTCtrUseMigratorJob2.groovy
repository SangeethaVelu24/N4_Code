package WBCT

import com.navis.argo.*
import com.navis.argo.business.api.ServicesManager
import com.navis.argo.business.atoms.*
import com.navis.argo.business.integration.IntegrationServiceMessage
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.model.GeneralReference
import com.navis.argo.business.model.LocPosition
import com.navis.argo.business.reference.*
import com.navis.cargo.business.api.BillOfLadingManager
import com.navis.cargo.business.model.BillOfLading
import com.navis.external.argo.AbstractGroovyJobCodeExtension
import com.navis.framework.business.Roastery
import com.navis.framework.business.atoms.LifeCycleStateEnum
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.persistence.hibernate.CarinaPersistenceCallback
import com.navis.framework.persistence.hibernate.PersistenceTemplate
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.BizFailure
import com.navis.framework.util.BizViolation
import com.navis.framework.util.DateUtil
import com.navis.framework.util.TransactionParms
import com.navis.framework.util.message.MessageCollector
import com.navis.framework.util.message.MessageCollectorFactory
import com.navis.inventory.InventoryEntity
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.api.UnitManager
import com.navis.inventory.business.atoms.*
import com.navis.inventory.business.imdg.HazardItem
import com.navis.inventory.business.imdg.Hazards
import com.navis.inventory.business.moves.MoveEvent
import com.navis.inventory.business.units.*
import com.navis.orders.business.api.OrdersFinder
import com.navis.orders.business.eqorders.Booking
import com.navis.orders.business.eqorders.EquipmentDeliveryOrder
import com.navis.orders.business.eqorders.EquipmentLoadoutOrder
import com.navis.orders.business.eqorders.EquipmentOrderItem
import com.navis.road.business.model.TruckingCompany
import com.navis.services.ServicesEntity
import com.navis.services.ServicesField
import com.navis.services.business.api.EventManager
import com.navis.services.business.rules.EventType
import com.navis.services.business.rules.FlagType
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import wslite.json.JSONArray
import wslite.json.JSONObject

class WBCTCtrUseMigratorJob2 extends AbstractGroovyJobCodeExtension {
    StringBuilder errorMessage = new StringBuilder()

    def ordersFinder = Roastery.getBean(OrdersFinder.BEAN_ID);
    def manager = Roastery.getBean(UnitManager.BEAN_ID);
    def em = Roastery.getBean(EventManager.BEAN_ID);
    private static final Logger LOGGER = Logger.getLogger(this.class)
    def servicesManager = Roastery.getBean(ServicesManager.BEAN_ID);

    @Override
    void execute(Map<String, Object> inParams) {
        LOGGER.setLevel(Level.DEBUG)
        LOGGER.warn("WBCTCtrUseMigratorJob2 started::::::::::::")
        List<Serializable> ismList = getMessagesToProcess()
        LOGGER.warn("WBCTCtrUseMigratorJob2 joblist size::::::::::::" + ismList?.size())
        for (Serializable ismGkey : ismList) {
            PersistenceTemplate pt = new PersistenceTemplate(getUserContext())
            pt.invoke(new CarinaPersistenceCallback() {
                protected void doInTransaction() {
                    IntegrationServiceMessage ism = (IntegrationServiceMessage) HibernateApi.getInstance().load(IntegrationServiceMessage.class, ismGkey);
                    MessageCollector mc = MessageCollectorFactory.createMessageCollector()
                    TransactionParms parms = TransactionParms.getBoundParms();
                    parms.setMessageCollector(mc)

                    errorMessage = new StringBuilder()
                    String unitId = ism.getIsmUserString1()
                    ism.setIsmUserString4(null)
                    String payload = null
                    if (StringUtils.isNotEmpty(ism.getIsmMessagePayload())) {
                        payload = ism.getIsmMessagePayload()
                    } else if (StringUtils.isNotEmpty(ism.getIsmMessagePayloadBig())) {
                        payload = ism.getIsmMessagePayloadBig()
                    }
                    if (StringUtils.isNotEmpty(payload)) {
                        JSONObject jsonObj = new JSONObject(payload);
                        try {
                            LOGGER.warn("log 1 unitId " + unitId)
                            Unit unit = processUnit(unitId, jsonObj)
                            //  if(unit == null){
                            if (getMessageCollector().hasError() && getMessageCollector().getMessageCount() > 0) {
                                errorMessage.append(getMessageCollector().toCompactString()).append("::")
                            }
                            if (errorMessage.toString().length() > 0) {
                                //    LOGGER.warn("logfordebugging 1.1 unitId " + errorMessage.toString())
                                ism.setIsmUserString4(errorMessage.take(100).toString())
                            } else if (unit != null) {
                                ism.setIsmUserString3("true")
                            }
                            HibernateApi.getInstance().save(ism)

                            //  LOGGER.warn("logfordebugging last unitId Done " + unitId)

                            //   }
                        } catch (Exception e) {
                            LOGGER.warn("e " + e)
                            errorMessage.append("" + e.getMessage().take(100)).append("::")
                            ism.setIsmUserString4(errorMessage.toString())
                        }

                    } else {
                        LOGGER.warn("No JSON Payload for " + ism.getIsmUserString1())
                    }

                }
            })
        }
    }


    def processUnit(unitId, jsonObj) {
        ContextHelper.threadDataSource = DataSourceEnum.SNX;

        Boolean migrateThroughContainers = Boolean.FALSE;

        String INBOUND_TRAIN = 'MIGRATION'
        String gkey = jsonObj.getOrDefault("gkey", null)
        gkey = (gkey && gkey != 'null') ? gkey : null;

        String equipmentId = jsonObj.getOrDefault("equipment_id", null)
        equipmentId = (equipmentId && equipmentId != 'null') ? equipmentId : null;

        String chassisId = jsonObj.getOrDefault("chassis_number", null)
        chassisId = (chassisId && chassisId != 'null') ? chassisId : null;

        String accessoryId = jsonObj.getOrDefault("accessory_number", null)
        accessoryId = (accessoryId && accessoryId != 'null') ? accessoryId : null;

        String category = jsonObj.getOrDefault("category", null)
        category = (category && category != 'null') ? category : null;
        UnitCategoryEnum cat = null
        if (!StringUtils.isEmpty(category)) {
            cat = getCategory(category);
            if (cat == null) {
                errorMessage.append("Categoty null." + category).append("::")
                return
            }
        } else {
            errorMessage.append("Categoty null." + category).append("::")
            return
        }
        if ((cat == UnitCategoryEnum.THROUGH) && !migrateThroughContainers) {
            errorMessage.append("Through Container, Not migrated.").append("::")
            return
        }

        String eqClass = jsonObj.getOrDefault("eq_class", null)
        eqClass = (eqClass && eqClass != 'null') ? eqClass : null;
        if (eqClass != 'CTR') {
            errorMessage.append("Equipment class not Container.").append("::")
            return
        }
        UnitFacilityVisit ufv = null
        Unit unit = null;
        EquipType equipmentType = null;
        Equipment equipment = null;
        Boolean isUnitMigratedBefore = Boolean.FALSE

        String isoCode = jsonObj.getOrDefault("iso_code", null)
        isoCode = (isoCode && isoCode != 'null') ? isoCode : null;

        if (equipmentId.startsWith('NC0')) {
            errorMessage.append("Non-containerised unit - skipped").append("::")
            return
        }

        equipment = Container.findContainer(equipmentId);
        if (equipment == null) {

            if (isoCode == '' || isoCode == null) {
                errorMessage.append("ISO Code is required for creating container " + equipmentId).append("::")
                return
            }

            equipmentType = EquipType.findEquipType(isoCode);
            if (equipmentType == null) {
                errorMessage.append("Equipment Type " + isoCode + " does not exist." + unitId).append("::")
                return
            }

            // equipment = this.createContainer(equipmentId, equipmentType);
            try {
                equipment = this.createContainer(equipmentId, equipmentType);
            } catch (BizViolation | BizFailure bv) {
                errorMessage.append("Error creating container" + bv.getLocalizedMessage());
                return
            }
            if (equipment == null) {
                errorMessage.append("Equipment of class " + eqClass + " with id " + equipmentId + " does not exist");
                return
            }
        }

        equipmentType = equipment.eqEquipType;
        //  LOGGER.warn("logfordebugging 200 unitId " + errorMessage.toString())
        if (equipmentType == null) {
            errorMessage.append("Cannot Find Equipment Type for " + equipmentId);
            return
        }

        Chassis chassis = null;

        if (chassisId && (chassisId != 'OWN')) {
            chassis = Chassis.findChassis(chassisId)
            if (chassis == null && equipmentType != null) {
                String chsType = null;
                EquipNominalLengthEnum eqLength = equipmentType.getEqtypNominalLength();
                if (eqLength == EquipNominalLengthEnum.NOM20) {
                    chsType = "20SF";
                } else if (eqLength == EquipNominalLengthEnum.NOM40) {
                    chsType = "40SF";
                }
                if (chsType) {
                    try {
                        chassis = Chassis.createChassis(chassisId, chsType, DataSourceEnum.SNX)
                    } catch (BizViolation | BizFailure bv) {
                        errorMessage.append("Error creating chassis" + bv.getLocalizedMessage());
                        return
                    }
                }

            }
        }


        Accessory accessory = null;
        if (accessoryId && accessoryId != 'null') {
            accessory = Accessory.findAccessory(accessoryId);
            if (accessory == null) {
                errorMessage.append("Accessory with id " + accessoryId + " does not exist");
                return
            }
        }

        if (ufv == null) {
            if (ufv) {
                unit = ufv.ufvUnit;
                if (ufv == null)
                    ufv = Unit.fndr.findPreadvisedUnit(ContextHelper.threadFacility, equipment);
            }
        }

        if (unit && ufv == null) {
            errorMessage.append("Unit for " + equipmentId + " is not null but ufv is null");
            return
        }


        String status = jsonObj.getOrDefault("status", null)
        status = (status && status != 'null') ? status : null;

        String ownerId = jsonObj.getOrDefault("owner_id", null)
        ownerId = (ownerId && ownerId != 'null') ? ownerId : null;

        String lineId = ownerId;

        // skip containers with operator = 'TEST'
        if (lineId == 'TEST') {
            errorMessage.append("Line Operator = TEST.  Not migrated.")
            return
        }
        // TODO check
        /*  if (lineId == 'HAP') {
              lineId = 'HLC'
          }*/
        String eqoLineId = jsonObj.getOrDefault("eqo_line_id", null)
        eqoLineId = (eqoLineId && eqoLineId != 'null') ? eqoLineId : null;

        //if booking exists then use those lines as container line
        lineId = (eqoLineId ? eqoLineId : lineId);

        if ("HAP".equalsIgnoreCase(lineId)) {
            lineId = "HLC"
        }

        if ("MED".equalsIgnoreCase(lineId)) {
            lineId = "MSC"
        }
        String podId1 = jsonObj.getOrDefault("pod1", null)
        podId1 = (podId1 && podId1 != 'null') ? podId1 : null;

        if (!StringUtils.isEmpty(podId1) && "INNAH".equalsIgnoreCase(podId1)) {
            podId1 = "INNVS"
        }

        String podId2 = jsonObj.getOrDefault("pod2", null)
        podId2 = (podId2 && podId2 != 'null') ? podId2 : null;
        if (!StringUtils.isEmpty(podId2) && "INNAH".equalsIgnoreCase(podId2)) {
            podId2 = "INNVS"
        }
        String polId = jsonObj.getOrDefault("pol", null)
        polId = (polId && polId != 'null') ? polId : null;


        String note = jsonObj.getOrDefault("note", null)
        note = (note && note != 'null') ? note : null;

        String cg_weight = jsonObj.getOrDefault("cg_weight", null)
        cg_weight = (cg_weight && cg_weight != 'null') ? cg_weight : null;

        String grossWeight = jsonObj.getOrDefault("gross_weight", null)
        grossWeight = (grossWeight && grossWeight != 'null') ? grossWeight : (cg_weight && cg_weight != 'null') ? cg_weight : null;

        String grossUnits = jsonObj.getOrDefault("gross_units", null)
        grossUnits = (grossUnits && grossUnits != 'null') ? grossUnits : null;

        String destination = jsonObj.getOrDefault("destination", null)
        destination = (destination && destination != 'null') ? destination : null;

        String origin = jsonObj.getOrDefault("origin", null)
        origin = (origin && origin != 'null') ? origin : null;

        String shandId = jsonObj.getOrDefault("shand_id", null)
        shandId = (shandId && shandId != 'null') ? shandId : null;

        String shippingMode = jsonObj.getOrDefault("shipping_mode", null)
        shippingMode = (shippingMode && shippingMode != 'null') ? shippingMode : null;

        String oogTop = jsonObj.getOrDefault("oog_top_cm", null)
        oogTop = (oogTop && oogTop != 'null') ? oogTop : null;

        String oogFront = jsonObj.getOrDefault("oog_front_cm", null)
        oogFront = (oogFront && oogFront != 'null') ? oogFront : null;

        String oogLeft = jsonObj.getOrDefault("oog_left_cm", null)
        oogLeft = (oogLeft && oogLeft != 'null') ? oogLeft : null;

        String oogRight = jsonObj.getOrDefault("oog_right_cm", null)
        oogRight = (oogRight && oogRight != 'null') ? oogRight : null;

        String oogBack = jsonObj.getOrDefault("oog_back_cm", null)
        oogBack = (oogBack && oogBack != 'null') ? oogBack : null;

        String inTime = jsonObj.getOrDefault("in_time", null)
        inTime = (inTime && inTime != 'null') ? inTime : null;

        String outTime = jsonObj.getOrDefault("out_time", null)
        outTime = (outTime && outTime != 'null') ? outTime : null;

        String created = jsonObj.getOrDefault("created", null)
        created = (created && created != 'null') ? created : null;

        String creator = jsonObj.getOrDefault("creator", null)
        creator = (creator && creator != 'null') ? creator : null;

        String temp = jsonObj.getOrDefault("temp_required_c", null)
        temp = (temp && temp != 'null') ? temp : null;

        String vent = jsonObj.getOrDefault("vent_required", null)
        vent = (vent && vent != 'null') ? vent : null;

        String ventUnit = jsonObj.getOrDefault("vent_units", null)
        ventUnit = (ventUnit && ventUnit != 'null') ? ventUnit : null;

        String group = jsonObj.getOrDefault("group_code_id", null)
        String groupId = (group && group != 'null') ? group : null

        group = groupId != null ? (groupId.endsWith(".") ? groupId.replace(".", "") : groupId) : null


        String drayStatus = jsonObj.getOrDefault("dray_status", null)
        drayStatus = (drayStatus && drayStatus != 'null') ? drayStatus : null;

        String seal1 = convertSealNbr(jsonObj.getOrDefault("seal_nbr1", null))
        String seal2 = convertSealNbr(jsonObj.getOrDefault("seal_nbr2", null))
        String seal3 = convertSealNbr(jsonObj.getOrDefault("seal_nbr3", null))
        String seal4 = convertSealNbr(jsonObj.getOrDefault("seal_nbr4", null))

        String drayTruckingCompany = jsonObj.getOrDefault("dray_trkc_id", null)
        drayTruckingCompany = (drayTruckingCompany && drayTruckingCompany != 'null') ? drayTruckingCompany : null;


        String remark = jsonObj.getOrDefault("remark", null)
        remark = (remark && remark != 'null') ? remark : null;

        String activeSparcs = jsonObj.getOrDefault("active_sparcs", null)
        activeSparcs = (activeSparcs && activeSparcs != 'null') ? activeSparcs : null;

        String locId = jsonObj.getOrDefault("loc_id", null)
        locId = (locId && locId != 'null') ? locId : null;

        String locType = jsonObj.getOrDefault("loc_type", null)
        locType = (locType && locType != 'null') ? locType : null;
        // MLB service code
        String unitFlexString05 = jsonObj.getOrDefault("unitFlexString05", null)
        unitFlexString05 = (unitFlexString05 && unitFlexString05 != 'null') ? unitFlexString05 : null;
        //OnOffdoc
        String unitFlexString02 = jsonObj.getOrDefault("unitFlexString02", null)
        unitFlexString02 = (unitFlexString02 && unitFlexString02 != 'null') ? unitFlexString02 : null;

        String vesselVisitId = jsonObj.getOrDefault("vessel_visit_id", null)
        vesselVisitId = (vesselVisitId && vesselVisitId != 'null') ? vesselVisitId : null;


        String position = jsonObj.getOrDefault("pos_id", null)
        // position = (position && position != 'null') ? position : null;
        //removing + or minus in the position
        position = (position && position != 'null') ? position.replaceAll(/\.\+$/, "").replaceAll(/\.\-$/, "") : null;


        String inPosId_ = jsonObj.getOrDefault("in_pos_id", null)
        inPosId_ = (inPosId_ && inPosId_ != 'null') ? inPosId_ : null;


        String inLocTypeId = jsonObj.getOrDefault("in_loc_type", null)
        inLocTypeId = (inLocTypeId && inLocTypeId != 'null') ? inLocTypeId : null;

        String inLocId = jsonObj.getOrDefault("in_loc_id", null)
        inLocId = (inLocId && inLocId != 'null') ? inLocId : null;


        String inCvId = jsonObj.getOrDefault("in_visit_id", null)
        inCvId = (inCvId && inCvId != 'null') ? inCvId : null;

        String arrivePosId = inPosId_;
        //String arrivePosId = document.@'arr_pos_id' ? document.@'arr_pos_id'.toString() : null;
        //arrivePosId = (arrivePosId && arrivePosId != 'null') ? arrivePosId : null;

        String arriveLocTypeId = inLocTypeId
        //String arriveLocTypeId = document.@'arr_loc_type' ? document.@'arr_loc_type'.toString() : null;
        //arriveLocTypeId = (arriveLocTypeId && arriveLocTypeId != 'null') ? arriveLocTypeId : null;

        String arriveLocId = inLocId
        //String arriveLocId = document.@'arr_loc_id' ? document.@'arr_loc_id'.toString() : null;
        //arriveLocId = (arriveLocId && arriveLocId != 'null') ? arriveLocId : null;

        String arriveCvId = inCvId
        //  LOGGER.warn("logfordebugging 400 unitId " + errorMessage.toString())
        //String arriveCvId = document.@'arr_visit_id' ? document.@'arr_visit_id'.toString() : null;
        //arriveCvId = (arriveCvId && arriveCvId != 'null') ? arriveCvId : null;


        //TODO -- check String service_status_code = jsonObj.getOrDefault("service_status_code", null) // todo check this field
        String departCvId = jsonObj.getOrDefault("dep_visit_id", null)
        departCvId = (departCvId && departCvId != 'null') ? departCvId : null;


        String departLocTypeId = jsonObj.getOrDefault("dep_loc_type", null)
        departLocTypeId = (departLocTypeId && departLocTypeId != 'null') ? departLocTypeId : null;

        String departLocId = jsonObj.getOrDefault("dep_loc_id", null)
        departLocId = (departLocId && departLocId != 'null') ? departLocId : null;

        String hubId = jsonObj.getOrDefault("hub_Id", null)
        hubId = (hubId && hubId != 'null') ? hubId : null;

        String outCvId = jsonObj.getOrDefault("out_visit_id", null)
        outCvId = (outCvId && outCvId != 'null') ? outCvId : null;


        String outPosId = jsonObj.getOrDefault("out_pos_id", null)
        outPosId = (outPosId && outPosId != 'null') ? outPosId : null;


        String outLocTypeId = jsonObj.getOrDefault("out_loc_type", null)
        outLocTypeId = (outLocTypeId && outLocTypeId != 'null') ? outLocTypeId : null;

        String outLocId = jsonObj.getOrDefault("out_loc_id", null)
        outLocId = (outLocId && outLocId != 'null') ? outLocId : null;


        //use map value if define in filter
        // String newisoCode = config.getFilterValueForKey("DM_ISO_CODES", isoCode);
        // Do not migrate non-containerised units (NC0)
        // TODO check this-- Long sortOrder = Long.parseLong((document.@'sort_order'.text() && document.@'sort_order'.text() != 'null')?document.@'sort_order'.text():null);
        // String ventUnit = jsonObj.getOrDefault("vent_units", null)
// todo check below
        /*  String group = document.@'group_code_id' ? document.@'group_code_id'.toString() : null;
          ;*/

        if (vesselVisitId == null || vesselVisitId.isEmpty()) {
            if (UnitCategoryEnum.IMPORT.equals(cat)) {
                vesselVisitId = inCvId;
            } else if (UnitCategoryEnum.EXPORT.equals(cat)) {
                vesselVisitId = departCvId;
            }
        }
        // todo check below
        if (vesselVisitId != null) {
            if (vesselVisitId.startsWith('DUMY')) {
                vesselVisitId = 'DUMY'
            } else if (vesselVisitId.startsWith('EMTY')) {
                vesselVisitId = 'EMTY'
            }
        }
        String tareWeight = jsonObj.getOrDefault("tare_weight", null)
        tareWeight = (tareWeight && tareWeight != 'null') ? tareWeight : null;

        String tareUnits = jsonObj.getOrDefault("tare_units", null)
        tareUnits = (tareUnits && tareUnits != 'null') ? tareUnits : null;
        // String tare_weight = jsonObj.getOrDefault("cntr_tare_weight", null)

        String safeWeight = jsonObj.getOrDefault("safe_weight", null)
        safeWeight = (safeWeight && safeWeight != 'null') ? safeWeight : null;

        String safeUnits = jsonObj.getOrDefault("safe_units", null)
        safeUnits = (safeUnits && safeUnits != 'null') ? safeUnits : null;

        /*if ((equipmentType.eqtypIsoGroup == EquipIsoGroupEnum.TD) ||
                (equipmentType.eqtypIsoGroup == EquipIsoGroupEnum.TG) ||
                (equipmentType.eqtypIsoGroup == EquipIsoGroupEnum.TN))*/
        // TODO check with Gopal on the above condition

        String commodityDescription = jsonObj.getOrDefault("commodity_description", null) //TODO trim
        commodityDescription = (commodityDescription && commodityDescription != 'null') ? commodityDescription : null;
        if (commodityDescription != null)
            commodityDescription = commodityDescription.trim();

        String eqoNumber = jsonObj.getOrDefault("eqo_nbr", null)
        eqoNumber = (eqoNumber && eqoNumber != 'null') ? eqoNumber : null;

        String eqoSubType = jsonObj.getOrDefault("eqo_sub_type", null)
        eqoSubType = (eqoSubType && eqoSubType != 'null') ? eqoSubType : null;

        LineOperator lineOperator = null
        String lineOp = lineId ? lineId : eqoLineId;

        String bl_count = jsonObj.getOrDefault("bl_count", null)
        Integer blCount = !StringUtils.isEmpty(bl_count) ? Integer.parseInt(bl_count) : null

        UfvTransitStateEnum ufvTransitState = this.getUfvTransitState(inTime, outTime, category, locType, blCount);
        UnitVisitStateEnum ufvVisitState = this.getUfvVisitState(ufvTransitState);

        FreightKindEnum fk = this.getFreightKind(status);
        if (cat == UnitCategoryEnum.STORAGE) {
            fk = FreightKindEnum.MTY;
        }
//  - Replace line operators for certain empties in yard
        /* if ((fk == FreightKindEnum.MTY) && (ufvTransitState == UfvTransitStateEnum.S40_YARD)) {
             if (yardEmptyLineSubst.containsKey(lineOp))
                 lineOp = yardEmptyLineSubst.get(lineOp)
         }*/

        if (ufvTransitState == UfvTransitStateEnum.S70_DEPARTED) {
            if (isUnitMigratedBefore)
                lineOperator = findLineOperator(ufv.ufvUnit.getUnitLineOperator().getBzuScac())
            else {
                errorMessage.append("Unit departed.  Not migrated.").append("::")
                return
            }
        } else {
            lineOperator = findLineOperator(lineOp);
            if (!locType) {
                errorMessage.append("Loc_type cannot be null $inTime , $outTime, $category, $locType");
                return
            }
        }

        if (lineOperator == null) {
            errorMessage.append(lineId + " is not a valid unit line operator");
            return
        }

        //  update equipment's owner to match line operator (which is equipment owner in Express)
        // todo check this -- if (equipment.eqTransponderId != "Fleet"){
        EquipmentState eqState = EquipmentState.findOrCreateEquipmentState(equipment, ContextHelper.getThreadOperator());
        if (eqState == null) {

            errorMessage.append("Failed to find or create equipment state");
            return

        } else {
            if (equipment.equipmentOwnerId != lineOperator.bzuId)
                eqState.upgradeEquipmentOwner(lineOperator, DataSourceEnum.SNX);
            if (equipment.equipmentOperatorId != lineOperator.bzuId)
                eqState.upgradeEquipmentOperator(lineOperator, DataSourceEnum.SNX);
            HibernateApi.instance.save(equipment);
        }
        // }

        if (chassis != null && lineOperator != null) {
            EquipmentState eqs = EquipmentState.findOrCreateEquipmentState(chassis, ContextHelper.getThreadOperator(),
                    lineOperator);
            if (chassis.getEquipmentOwnerId() != lineOperator.bzuId)
                eqs.upgradeEquipmentOwner(lineOperator, DataSourceEnum.SNX);
            if (chassis.getEquipmentOperatorId() != lineOperator.bzuId)
                eqs.upgradeEquipmentOperator(lineOperator, DataSourceEnum.SNX);
            HibernateApi.instance.save(eqs);
        }


        // todo chek --use substitute if mapped
        // lineOp = util.getLineOpSubst(lineOp)
        //  DM - Filter obsolete Line operators from migration
        /*if (util.isExcludedLine(lineOp))
            throw new Exception("LINE_ID $lineOp is obsolete");*/


        RoutingPoint pol = null;

        if (polId) {
            pol = RoutingPoint.resolveRoutingPointFromEncoding(UnLoc, polId);
            if (pol == null) {
                pol = RoutingPoint.findRoutingPoint(polId)
                /*if (pol == null) {
                    if (polId != null || polId != '') {
                        *//* String newpolId = config.getFilterValueForKey("DM_UN_LOC_CODES", polId);
                         if (newpolId) {
                             pol = RoutingPoint.findRoutingPoint(newpolId);
                         }*//*
                    }
                }*/
                if (pol == null)
                    pol = RoutingPoint.findRoutingPoint('UNK')
            }
        }

        RoutingPoint pod1 = null;
        if (podId1) {
            if (cat == UnitCategoryEnum.IMPORT)
                pod1 = RoutingPoint.findRoutingPoint('LAX')
            else {
                pod1 = RoutingPoint.resolveRoutingPointFromEncoding(UnLoc, podId1);
                if (pod1 == null) {
                    pod1 = RoutingPoint.findRoutingPoint(podId1)
                }
            }

            if (pod1 == null) {
                if (podId1 != null || podId1 != '') {
                    /* String newpodId1 = config.getFilterValueForKey("DM_UN_LOC_CODES",podId1) ;
                     if (newpodId1)
                         pod1 = RoutingPoint.findRoutingPoint(newpodId1);*/
                    if (pod1 == null)
                        pod1 = RoutingPoint.resolveRoutingPointFromUnLoc(podId1)
                }
            }
            if (pod1 == null)
                errorMessage.append("Invalid pod1 - ${podId1}").append("::")
        }

        RoutingPoint pod2 = null;
        //   LOGGER.warn("logfordebugging 600 unitId " + errorMessage.toString())
        if (podId2) {
            pod2 = RoutingPoint.resolveRoutingPointFromEncoding(UnLoc, podId2);
            if (pod2 == null) {
                pod2 = RoutingPoint.findRoutingPoint(podId2);
                /*if (pod2 == null) {
                    if (podId2 != null || podId2 != '') {
                        String newpodId2 = config.getFilterValueForKey("DM_UN_LOC_CODES", podId2);
                        if (newpodId2) {
                            pod2 = RoutingPoint.findRoutingPoint(newpodId2);
                        }
                    }
                }*/
            }
        }

        if (arriveLocTypeId == null || arriveLocTypeId == '') {
            // set arriveLocTypeID to the in values
            arriveLocTypeId = inLocTypeId;
            arriveCvId = inCvId;
            arriveLocId = inLocId;
        }

        if (!arriveLocTypeId) {
            arriveLocTypeId = "T";
            arriveLocId = "TRUCK";
            //throw new Exception("Arrival Loc Type id is required in order to assign the N4 Declared I/B Visit to " + equipmentId);
        }


        //
        if ((arriveLocTypeId == "R") && (arriveCvId == null || arriveCvId == '')) {
            arriveCvId = "GEN_TRAIN";
        }

        if ((arriveLocTypeId == "T") && (arriveLocId == null || arriveLocId == '')) {
            arriveLocId = "GEN_TRUCK";
        }

/////////////////////////  declared IB //////////////////////
        CarrierVisit declaredIbcv;
        if (arriveLocTypeId == "Y" && (arriveCvId == null || arriveCvId == '' || arriveCvId == 'null' || arriveCvId.isEmpty()))
            declaredIbcv = getGenericCarrierVisit('V');
        else if ((arriveLocTypeId == "Y") && ("CFS".equals(arriveCvId) || "ONHIRE".equals(arriveCvId)))
            declaredIbcv = getGenericCarrierVisit('V');
        else if (arriveLocTypeId == 'R') {
            // (ufvTransitState == UfvTransitStateEnum.S20_INBOUND || ufvTransitState == UfvTransitStateEnum.S10_ADVISED))
            declaredIbcv = this.getCarrierVisit(arriveLocTypeId, arriveCvId, arriveLocId, inCvId);
            /*else if (arriveLocTypeId == 'R' &&
                ufvTransitState >= UfvTransitStateEnum.S40_YARD)
            declaredIbcv = this.getCarrierVisit(arriveLocTypeId, INBOUND_TRAIN, INBOUND_TRAIN, INBOUND_TRAIN)*/
        } else
            declaredIbcv = this.getCarrierVisit(arriveLocTypeId, arriveCvId, arriveLocId, vesselVisitId);

        if (declaredIbcv == null && arriveLocTypeId == 'V')
            declaredIbcv = getGenericCarrierVisit('V');


        if (declaredIbcv == null) {
            errorMessage.append("Declared I/B CV cannot be found or created using arriveLocTypeId: " + arriveLocTypeId + " /arriveCvId: " + arriveCvId + " /arriveLocId: " + arriveLocId + " /VesselVisitId: " + vesselVisitId + " for " + equipmentId);
            return
        }

////////////////////  actual IB //////////////////
        CarrierVisit actualIbcv;
        if ((inLocTypeId == "Y" || inLocTypeId == null || inLocTypeId == 'V') && (inCvId == null || inCvId == '' || inCvId == 'null' || inCvId.isEmpty()))
        //if actual ibcv is blank, copy declared ibcv into actual
            actualIbcv = declaredIbcv;
        else if ((inLocTypeId == "Y") && ("CFS".equals(inCvId) || "ONHIRE".equals(inCvId)))
            actualIbcv = getGenericCarrierVisit('V');
        else if (arriveLocTypeId == 'R') {
            // (ufvTransitState == UfvTransitStateEnum.S20_INBOUND || ufvTransitState == UfvTransitStateEnum.S10_ADVISED))
            LOGGER.debug("arriveLocTypeId " + arriveLocTypeId + "arriveCvId" + arriveCvId + "arriveLocId" + arriveLocId + "inCvId" + inCvId)
            actualIbcv = this.getCarrierVisit(arriveLocTypeId, arriveCvId, arriveLocId, inCvId);
            /*else if (arriveLocTypeId == 'R' &&
                ufvTransitState >= UfvTransitStateEnum.S40_YARD)
            actualIbcv = this.getCarrierVisit(arriveLocTypeId, INBOUND_TRAIN, INBOUND_TRAIN, INBOUND_TRAIN)*/
        } else
            actualIbcv = this.getCarrierVisit(inLocTypeId, inCvId, inLocId, vesselVisitId);


        if (actualIbcv == null)
            actualIbcv = getGenericCarrierVisit(inLocTypeId);

        if (actualIbcv == null) {
            errorMessage.append("Actual I/B CV cannot be found or created").append("::")
            return
        }

        // BI 212956 - if unit is storage empty, set declared and actual o/b to "GEN_CARRIER"

        CarrierVisit declaredObcv, actualObcv
        if ((cat == UnitCategoryEnum.STORAGE) && (fk == FreightKindEnum.MTY)) {
            DomainQuery dq = QueryUtils.createDomainQuery("CarrierVisit")
            dq.addDqPredicate(PredicateFactory.eq(ArgoField.CV_ID, "GEN_CARRIER"))
            dq.addDqPredicate(PredicateFactory.eq(ArgoField.CV_CARRIER_MODE, LocTypeEnum.UNKNOWN))
            declaredObcv = (CarrierVisit) Roastery.getHibernateApi().getUniqueEntityByDomainQuery(dq)
            if (declaredObcv == null) {
                errorMessage.append("GEN_CARRIER/UNKNOWN carrier visit not found!").append("::")
                return
            }
            actualObcv = declaredObcv
        } else {
            ////////////// declared OB ////////////////////
            // This is required
            declaredObcv = departLocTypeId ? this.getCarrierVisit(departLocTypeId, departCvId, departLocId, vesselVisitId) : null;
            if (declaredObcv == null) {
                if (departLocTypeId == 'V')
                    declaredObcv = getGenericCarrierVisit('V');
                if (departLocTypeId == 'T')
                    declaredObcv = getGenericCarrierVisit('T');
                if (departLocTypeId == 'R')
                    declaredObcv = getGenericCarrierVisit('R');
            }

            if (declaredObcv == null)
                declaredObcv = getGenericCarrierVisit('T');

            //////////// actual OB -- mostly NULL, since migration is done only for Active inYARD units //////////////////////////////
            actualObcv = outLocTypeId ? this.getCarrierVisit(outLocTypeId, outCvId, outLocId, vesselVisitId) : null
        }

        if (ufv == null) {
            LOGGER.debug("Trying to create or find preadvised unit")
            ufv = manager.findOrCreatePreadvisedUnit(ContextHelper.threadFacility,
                    equipmentId,
                    equipmentType,
                    cat,
                    this.getFreightKind(status),
                    lineOperator,
                    declaredIbcv,
                    declaredObcv,
                    ContextHelper.threadDataSource,
                    remark,
                    pod1,
                    null, //inEquipmentOperator
                    null, // shipper
                    null, //truckingCompany
                    ufvVisitState == UnitVisitStateEnum.ACTIVE);
            LOGGER.debug("Created preadvised unit")
            // TODO check the existing  ufvTransitState == UnitVisitStateEnum.ACTIVE)
        } else if (ufv.ufvUnit.unitLineOperator.bzuId != lineOperator.bzuId)
            ufv.ufvUnit.updateLineOperator(lineOperator)

        unit = ufv.ufvUnit;

        // addToPADMFlexData(ufv, gkey);

        if (ufv == null) {
            errorMessage.append("Cannot create UFV for EQ Use gkey = " + gkey).append("::")
            return
        }

        if (unit.unitActiveUfv != ufv)
            unit.unitActiveUfv = ufv;

        if (ufv.ufvVisibleInSparcs != (activeSparcs != ''))
            ufv.ufvVisibleInSparcs = (activeSparcs != '');

        attachDamages(unit, jsonObj);

        def routing = unit.ensureUnitRouting();
        if (hubId != null) {
            unit.unitFlexString01 = hubId
        }
        if (unitFlexString05 != null) {
            unit.unitFlexString05 = unitFlexString05;
        }
        if (unitFlexString02 != null) {
            unit.unitFlexString02 = unitFlexString02;
        }
        if (unit.unitRemark != remark)
            unit.unitRemark = remark;

        if (ufv.ufvSparcsNote != note)
            ufv.ufvSparcsNote = note;

        if (ufv.ufvTransitState != ufvTransitState)
            ufv.ufvTransitState = ufvTransitState;

        if (ufv.ufvVisitState != ufvVisitState)
            ufv.ufvVisitState = ufvVisitState;

        if (ufv.isTransitState(UfvTransitStateEnum.S10_ADVISED))
            ufv.ufvVisibleInSparcs = false

        if (unit.unitVisitState != ufv.ufvVisitState)
            unit.unitVisitState = ufv.ufvVisitState;

        if (inTime) {
            Date t = inTime ? parseDate(inTime) : null;

            if (ufv.ufvTimeIn != t)
                ufv.ufvTimeIn = t

            if (ufv.ufvTimeInventory != ufv.ufvTimeIn)
                ufv.ufvTimeInventory = ufv.ufvTimeIn
        }

        if (outTime) {
            def ot = outTime ? parseDate(outTime) : null;

            if (ufv.ufvTimeOut != ot)
                ufv.ufvTimeOut = ot;

            if (ufv.ufvTimeOfLoading != ufv.ufvTimeOut)
                ufv.ufvTimeOfLoading = ufv.ufvTimeOut;

            if (ufv.ufvTimeOfLastMove != ot)
                ufv.ufvTimeOfLastMove = ot;

            if (ufv.ufvTimeComplete != ot)
                ufv.ufvTimeComplete = ot;
        }

        if (unit.unitDeclaredIbCv != declaredIbcv)
            unit.unitDeclaredIbCv = declaredIbcv

        if (ufv.ufvActualIbCv != actualIbcv)
            ufv.ufvActualIbCv = actualIbcv

        CarrierVisit dcv = null;
        if (declaredObcv && CarrierVisit.isGenericId(declaredObcv.cvId) && actualObcv)
            dcv = actualObcv;
        else
            dcv = declaredObcv;

        if (dcv == null)
            dcv = CarrierVisit.getGenericCarrierVisit(ContextHelper.threadComplex);

        if (routing.rtgDeclaredCv != dcv)
            routing.rtgDeclaredCv = dcv;

        if (ufv.ufvActualObCv != actualObcv)
            ufv.ufvActualObCv = actualObcv;

        if (ufv.ufvIntendedObCv != dcv)
            ufv.ufvIntendedObCv = dcv;

        if (ufv.ufvActualObCv == null && routing.rtgDeclaredCv)
            ufv.ufvActualObCv = routing.rtgDeclaredCv;

        if (routing.rtgPOD1 != pod1)
            routing.rtgPOD1 = pod1;

        if (routing.rtgPOD2 != pod2)
            routing.rtgPOD2 = pod2;

        if (routing.rtgPOL != pol)
            routing.rtgPOL = pol

        if (created) {
            Date c = created ? parseDate(created) : null;

            if (ufv.ufvCreateTime != c)
                ufv.ufvCreateTime = c;
        }

        Long h = new Long(DateUtil.HORIZON_MAX)

        if (ufv.ufvHorizon != h)
            ufv.ufvHorizon = h;

        GoodsBase goods = unit.ensureGoods();

        if (commodityDescription && (goods.gdsCommodity == null || (goods.gdsCommodity && goods.gdsCommodity.cmdyDescription != commodityDescription))) {
            Commodity commodity = Commodity.findOrCreateCommodity(commodityDescription.trim());
            commodity.cmdyDescription = commodityDescription;
            commodity.cmdyLifeCycleState = LifeCycleStateEnum.ACTIVE;
            goods.gdsCommodity = commodity;
        }

        if (pol && routing && routing.rtgPOL != pol)
            routing.rtgPOL = pol;

        if (pod2 && routing && routing.rtgPOD2 != pod2)
            routing.rtgPOD2 = pod2;
        String rail_destination = jsonObj.getOrDefault("rail_destination", null)
        if (ufv.ufvActualObCv != null && ufv.getUfvActualObCv().getLocType() == LocTypeEnum.TRAIN) {
            destination = rail_destination
        }
        if (destination != null && destination.length() > 15)
            destination = destination.substring(0, 14)
        if (destination && unit.unitGoods && unit.unitGoods.gdsDestination != destination)
            unit.unitGoods.gdsDestination = destination;

        if (arriveLocTypeId == 'R' && actualIbcv && actualIbcv.locType != LocTypeEnum.YARD) {
            String locIdR = actualIbcv.locId
            String posIdR = arrivePosId
            /* if(inPosId_ != null && inPosId_.contains("-")){
                 String[] positionParsed = position.split("-");
                 locIdR = positionParsed[0]
                 posIdR = positionParsed[1]
             }
             log("locIdR "+locIdR)
             log("posIdR "+posIdR)*/
            ufv.ufvArrivePosition = LocPosition.createLocPosition(actualIbcv.locType, locIdR, actualIbcv.locGkey, posIdR, null);
        } else if (actualIbcv && actualIbcv.locType != LocTypeEnum.YARD) {
            ufv.ufvArrivePosition = LocPosition.createLocPosition(actualIbcv.locType, actualIbcv.locId, actualIbcv.locGkey, arrivePosId, null);
        } else if (declaredIbcv && declaredIbcv.locType != LocTypeEnum.YARD) {
            ufv.ufvArrivePosition = LocPosition.createLocPosition(declaredIbcv.locType, declaredIbcv.locId, declaredIbcv.locGkey, inPosId_, null);
        }

        if (ufv.ufvTransitState == UfvTransitStateEnum.S40_YARD) {
            ufv.ufvLastKnownPosition = LocPosition.createYardPosition(ContextHelper.threadYard, position, null, equipmentType.eqtypBasicLength, false);
            //Set CWP flag for empty overflow yard locations
            if (position?.matches('^[0-9]{1,3}P.*'))
                unit.unitFlexString03 = 'CWP'

        } else if ([UfvTransitStateEnum.S60_LOADED, UfvTransitStateEnum.S70_DEPARTED].contains(ufv.ufvTransitState)) {
            CarrierVisit ccvv = actualObcv ? actualObcv : declaredObcv;
            if (ccvv)
                ufv.ufvLastKnownPosition = LocPosition.createLocPosition(ccvv.locType, ccvv.locId, ccvv.locGkey, outPosId, null);
        } else if (ufv.ufvTransitState == UfvTransitStateEnum.S10_ADVISED || ufv.ufvTransitState == UfvTransitStateEnum.S20_INBOUND) {
            CarrierVisit ccvv1 = actualIbcv ? actualIbcv : declaredIbcv;
            if (ccvv1)
                ufv.ufvLastKnownPosition = LocPosition.createLocPosition(ccvv1.locType, ccvv1.locId, ccvv1.locGkey, inPosId_, null);
        }
        //  LOGGER.warn("logfordebugging 2  before gross weight in ctr use migrator")cr
        checkUnitEquipmentState(unit);

        if (grossWeight && grossUnits) {
            Double w = this.convertWeight(grossWeight, grossUnits);

            if (unit.unitGoodsAndCtrWtKg != w)
                unit.unitGoodsAndCtrWtKg = w;
        }
        //  LOGGER.warn("logfordebugging 2  before tare weight in ctr use migrator")
        if (unit.unitEquipment.tareWeightUpdateAllowed) {

            if (tareWeight && tareUnits) {
                Double tare = this.convertWeight(tareWeight, tareUnits);
                if (fk == FreightKindEnum.MTY && unit.unitGoodsAndCtrWtKg != null && unit.unitGoodsAndCtrWtKg > tare) {
                    tare = unit.unitGoodsAndCtrWtKg
                    tareUnits = 'KG'
                }


                if (unit.unitEquipment.eqTareWeightKg != tare) {
                    unit.unitEquipment.eqTareWeightKg = tare;
                }
            } else if (fk == FreightKindEnum.MTY && unit.unitGoodsAndCtrWtKg != null) {
                unit.unitEquipment.eqTareWeightKg = unit.unitGoodsAndCtrWtKg;
            }
        }


        if (safeWeight && safeUnits) {
            Double safe = this.convertWeight(safeWeight, safeUnits);
            if (unit.unitEquipment.eqSafeWeightKg != safe) {
                unit.unitEquipment.eqSafeWeightKg = safe;
            }
        }
        if (unit.unitEquipment.eqSafeWeightKg == 0 && equipmentType != null && equipmentType.getEqtypSafeWeightKg() != null) {
            unit.unitEquipment.updateEqSafeWtKg(equipmentType.getEqtypSafeWeightKg())
            //unit.unitEquipment.eqSafeWeightKg = equipmentType.getEqtypSafeWeightKg()
        }


        ufv.ufvFlexString09 = null;
        if (unit.unitGoodsAndCtrWtKg) {
            Double grWeight = unit.getUnitGoodsAndCtrWtKg()
            grWeight = grWeight / 100
            ufv.ufvFlexString09 = (Math.round(grWeight) / 10).toString();
        }

        //Add Shipping Mode
        if (unit.unitFlexString04 != shippingMode)
            unit.unitFlexString04 = shippingMode;

        if (oogTop) {
            Double ot = Double.valueOf(oogTop);

            if (unit.unitOogTopCm != ot)
                unit.unitOogTopCm = ot;
        }

        if (oogFront) {
            Double of = Double.valueOf(oogFront)

            if (unit.unitOogFrontCm != of)
                unit.unitOogFrontCm = of;
        }

        if (oogBack) {
            Double ob = Double.valueOf(oogBack);

            if (unit.unitOogBackCm != ob)
                unit.unitOogBackCm = ob;
        }

        if (oogLeft) {
            Double ol = Double.valueOf(oogLeft);

            if (unit.unitOogLeftCm != ol)
                unit.unitOogLeftCm = ol
        }

        if (oogRight) {
            Double or = Double.valueOf(oogRight)

            if (unit.unitOogRightCm != or)
                unit.unitOogRightCm = or;
        }

        Boolean isOog = oogTop || oogFront || oogBack || oogLeft || oogRight;

        if (unit.unitIsOog != isOog)
            unit.unitIsOog = isOog;

        if (goods.gdsOrigin != origin)
            goods.gdsOrigin = origin;

        if (temp || vent || ventUnit) {
            if (equipmentType.isTemperatureControlled()) {
                def reeferRequirements = goods.ensureGdsReeferRqmnts()
                Double t = temp ? Double.valueOf(temp) : null

                if (reeferRequirements.rfreqTempRequiredC != t)
                    reeferRequirements.rfreqTempRequiredC = t;
                if (_ventReqSubst.containsKey(vent))
                    vent = _ventReqSubst.get(vent)
                Double v = vent ? Double.valueOf(vent) : null;


                if (reeferRequirements.rfreqVentRequired != v)
                    reeferRequirements.rfreqVentRequired = v;

                VentUnitEnum vt = ventUnit ? this.getVentilationUnit(ventUnit) : null;

                if (reeferRequirements.rfreqVentUnit != vt)
                    reeferRequirements.rfreqVentUnit = vt;

                if (unit.unitRequiresPower == false)
                    unit.unitRequiresPower = true;
                //BI 209423
                if (unit.locType == LocTypeEnum.YARD && unit.getUnitFreightKind() == FreightKindEnum.FCL) {
                    if (unit.unitIsPowered == false)
                        unit.unitIsPowered = true;
                }
            }
        }

        if (group) {
            Group groupCode = Group.findOrCreateGroup(group);

            if (groupCode == null)
                errorMessage.append("Unknown group code " + group).append("::")

            if (groupCode != routing.rtgGroup)
                routing.rtgGroup = groupCode;
        }

        if (drayTruckingCompany) {
            TruckingCompany tc = TruckingCompany.findOrCreateTruckingCompany(drayTruckingCompany);

            if (tc == null) {
                errorMessage.append("Unknown Trucking Company " + drayTruckingCompany);
            }

            if (routing.rtgTruckingCompany != tc)
                routing.rtgTruckingCompany = tc
        }

        if (drayStatus) {
            DrayStatusEnum ds = this.getDrayStatus(drayStatus);

            if (ds != unit.unitDrayStatus)
                unit.unitDrayStatus = ds;
        }

        def event = em.getMostRecentEventByType(EventType.resolveIEventType(EventEnum.UNIT_CREATE), unit);

        if (seal1)
            unit.unitSealNbr1 = seal1;
        if (seal2)
            unit.unitSealNbr2 = seal2;
        if (seal3)
            unit.unitSealNbr3 = seal3;
        if (seal4)
            unit.unitSealNbr4 = seal4;

        if (event) {
            event.evntAppliedDate = created ? parseDate(created) : null;
            event.evntCreator = creator;
            event.evntAppliedBy = creator;
            event.evntCreated = event.evntAppliedDate;
            event.evntChanger = creator;
            event.evntChanged = event.evntAppliedDate;
        }

        if (isoCode?.reverse()?.take(2)?.reverse() == 'TR')
            ufv.ufvFlexString10 = 'TL'
        else if (chassis)
            ufv.ufvFlexString10 = 'CC'
        else
            ufv.ufvFlexString10 = 'CN'

        LOGGER.warn("logfordebugging 3 before processing haz in ctr use")
        String haz_count = jsonObj.getOrDefault("haz_count", null)

        if (!StringUtils.isEmpty(haz_count)) {
            Integer hazCount = Integer.valueOf(haz_count)


            if (hazCount != null && hazCount > 0) {
                List<JSONArray> hazList = (List<JSONArray>) jsonObj.getOrDefault("hazard-list", null)
                if (hazList != null && hazList.size() > 0) {
                    JSONArray jarray = hazList.get(0)
                    Hazards hazards = Hazards.createHazardsEntity()


                    for (int i = 0; i < hazCount; i++) {
                        JSONObject hazObj = jarray.getJSONObject(i)
                        if (hazObj != null) {
                            String imdg = hazObj.getOrDefault("imdg_id", null)
                            imdg = (imdg && imdg != 'null') ? imdg : null;

                            String unNbr = hazObj.getOrDefault("undg_nbr", null)
                            unNbr = (unNbr && unNbr != 'null') ? unNbr.padLeft(4, '0') : null;

                            /*  String imdg1 = config.getFilterValueForKey("DM_IMDG_CODES", imdg);
                              if (imdg1 != null)
                                  imdg = imdg1;*/
                            HazardItem hi = null
                            if (imdg != null && !imdg.isAllWhitespace()) {
                                hi = hazards.addHazardItem(imdg, unNbr);
                            }
                            //Do not raise error when IMDG is null or empty and container is not in the yard.
                            else if (ufv.isTransitState(UfvTransitStateEnum.S40_YARD)) {
                                errorMessage.append("Null or empty IMDG class for haz record" + "UNNO: " + unNbr).append("::")
                            }
                            if (hi != null) {
                                // Weight
                                String weight_ = hazObj.getOrDefault("weight", null)
                                String weightUnit_ = hazObj.getOrDefault("weight_units", null)


                                Double grossWeight_ = null;

                                if (weight_ && weightUnit_)
                                    grossWeight_ = this.convertWeight(weight_, weightUnit_);

                                if (hi.hzrdiWeight != grossWeight_)
                                    hi.hzrdiWeight = grossWeight_;

                                // Quantity
                                Long quantity_ = (hazObj.getOrDefault("qty", null) != null && hazObj.getOrDefault("qty", null) instanceof Long) ? hazObj.getLong("qty") : null

                                if (hi.hzrdiQuantity != quantity_)
                                    hi.hzrdiQuantity = quantity_;

                                // Sequence
                                String sequenc = hazObj.getOrDefault("seq", null)
                                Long sequence_ = !StringUtils.isEmpty(sequenc) && sequenc instanceof Long ? hazObj.getLong("seq") : null

                                if (hi.hzrdiSeq != sequence_)
                                    hi.hzrdiSeq = sequence_;

                                // Proper Name
                                String properName = hazObj.getOrDefault("proper_name", null)

                                if (hi.hzrdiProperName != properName)
                                    hi.hzrdiProperName = properName;

                                // Technical Name
                                String technicalName = hazObj.getOrDefault("technical_name", null)

                                if (hi.hzrdiTechName != technicalName)
                                    hi.hzrdiTechName = technicalName;

                                // Inhalation Zone
                                String zone = hazObj.getOrDefault("inhalation_zone", null)

                                if (hi.hzrdiInhalationZone != zone)
                                    hi.hzrdiInhalationZone = zone;

                                // IMDG Page
                                String page = hazObj.getOrDefault("imdg_page", null)

                                if (hi.hzrdiPageNumber != page)
                                    hi.hzrdiPageNumber = page;

                                // Package Type
                                String packageType = hazObj.getOrDefault("package_type", null)

                                if (hi.hzrdiPackageType != packageType)
                                    hi.hzrdiPackageType = packageType;

                                // EMS Number
                                String ems = hazObj.getOrDefault("ems_nbr", null)

                                if (hi.hzrdiEMSNumber != ems)
                                    hi.hzrdiEMSNumber = ems;

                                /*  // Emergency Telephone
                                 String weight_ = hazObj.getOrDefault("weight", null)
                              String telephone = haz.@'contact_phone'.text();
                              if (hi.hzrdiEmergencyTelephone != telephone)
                                  hi.hzrdiEmergencyTelephone = telephone;*/

                                // MFAG
                                String mfag = hazObj.getOrDefault("mfag_nbr", null)

                                if (hi.hzrdiMFAG != mfag)
                                    hi.hzrdiMFAG = mfag;

                                // Flash Point -- 'flash_point_units' <-- I assume it is always C

                                String fp = hazObj.getOrDefault("flash_point", null)
                                Double flashPoint = null;
                                if (fp != null && fp.length() > 0) {
                                    fp = fp.replaceAll(/[^0-9.-]/, '')
// for testing purpose, have to correct this during extraction itself
                                    flashPoint = Double.parseDouble(fp)
                                }

                                if (hi.hzrdiFlashPoint != flashPoint)
                                    hi.hzrdiFlashPoint = flashPoint;


                                // Limited Quantity
                                String limitedQuantity = hazObj.getOrDefault("limited_qty_flag", null)

                                Boolean limitedQuantity_ = (limitedQuantity && limitedQuantity == 'X');
                                if (hi.hzrdiLtdQty != limitedQuantity_)
                                    hi.hzrdiLtdQty = limitedQuantity_;

                                // Marine Polutant
                                String mp = hazObj.getOrDefault("marine_pollutant", null)

                                Boolean mp_ = (mp && mp == 'X');
                                if (hi.hzrdiMarinePollutants != mp_)
                                    hi.hzrdiMarinePollutants = mp_;

                                // Packing Group
                                String pg = hazObj.getOrDefault("packing_group", null)

                                HazardPackingGroupEnum pg_ = null;
                                if (pg)
                                    pg_ = HazardPackingGroupEnum.getEnum(pg);

                                if (hi.hzrdiPackingGroup != pg_)
                                    hi.hzrdiPackingGroup = pg_;

                                String name = hazObj.getOrDefault("emergency_contact", null)
                                name = (name && name != 'null') ? name : '';

                                String phone = hazObj.getOrDefault("contact_phone", null)
                                phone = (phone && phone != 'null') ? phone : '';

                                hi.hzrdiEmergencyTelephone = (phone + ' ' + name).trim().take(20);
                            }
                        }
                        unit.attachHazards(hazards);
                    }
                }
                //unit.attachHazards(hazards);
            }
        }
        //if (ufv.ufvVisitState == UnitVisitStateEnum.ACTIVE) {
        LOGGER.warn("logfordebugging 4 before processing holds in cte use")
        String hcount = jsonObj.getOrDefault("hold_count", null)

        if (!StringUtils.isEmpty(hcount)) {
            int holdCount = Integer.valueOf(hcount)


            if (holdCount > 0) {
                List<JSONArray> holdList = (List<JSONArray>) jsonObj.getOrDefault("hold-list", null)
                if (holdList != null && holdList.size() > 0) {
                    JSONArray holdarray = holdList.get(0)

                    log("found holds, processing it");
                    for (int i = 0; i < holdCount; i++) {
                        JSONObject holdObj = holdarray.getJSONObject(i)
                        if (holdObj != null) {
                            String flagId_ = holdObj.getOrDefault("hold_type_id", null)
                            String holdAppliedDate = holdObj.getOrDefault("hold_date", null)
                            String holdReleasedDate = holdObj.getOrDefault("release_date", null)
                            Date applyDate_ = holdAppliedDate ? parseDate(holdAppliedDate) : null
                            Date releaseDate_ = holdReleasedDate ? parseDate(holdReleasedDate) : null

                            String holdReference_ = holdObj.getOrDefault("hold_reference", null)
                            String releaseReference_ = holdObj.getOrDefault("release_reference", null)
                            String heldBy_ = holdObj.getOrDefault("held_by", null)
                            String releasedBy_ = holdObj.getOrDefault("released_by", null)


//TODO -- check gkey field
                            // Long gkey_ = (hold.@'gkey'.text() && hold.@'gkey'.text() != 'null') ? Long.parseLong(hold.@'gkey'.text()) : null;
                            String holdCreated = holdObj.getOrDefault("created", null)
                            String creator_ = jsonObj.getOrDefault("creator", null)

                            Date created_ = holdCreated ? parseDate(holdCreated) : null

                            String changer_ = holdObj.getOrDefault("changer", null)
                            String holdChanged = jsonObj.getOrDefault("changed", null)


                            Date changed_ = holdChanged ? parseDate(holdChanged) : null

                            if (flagId_ != null && flagId_ != '') {
                                flagId_ = flagId_.toUpperCase();
                            }
                            FlagType fg = FlagType.findFlagType(flagId_);
                            // Apply hold to unit
                            if (fg) {
                                log("Hold type exists, trying to create it");
                                if (fg.flgtypAppliesTo == LogicalEntityEnum.UNIT)
                                    if (!fg.flgtypIsBillingHoldRequired) {

                                        if (applyDate_ && fg.flgtypPurpose != FlagPurposeEnum.PERMISSION)
                                            applyHoldUnitEquipment(flagId_, unit, null, releaseReference_ ? releaseReference_ : holdReference_, applyDate_, heldBy_);

                                        if (releaseDate_) {
                                            if (fg.flgtypPurpose == FlagPurposeEnum.HOLD)
                                                releaseHoldUnitEquipment(flagId_, unit, null, releaseReference_, releaseDate_, releasedBy_);
                                            else //Grant permission if Express has corresponding hold released
                                                servicesManager.applyPermission(flagId_, unit, releaseReference_, 'Applied by DM', true);
                                        }
                                    } else //apply events if the billing hold is active
                                    {
                                        if (fg.flgtypPurpose == FlagPurposeEnum.HOLD && !releaseDate_) {
                                            def evnt = this.recordEvent(flagId_, "Hold applied by $heldBy_", unit, null)

                                            if (evnt) {
                                                //TODO -- check gkey field
                                                //to prevent duplicate events during incremental migration
                                                /*  if (evnt.evntBillingExtractBatchId != gkey_)
                                                      evnt.evntBillingExtractBatchId = gkey_;*/

                                                if (heldBy_ == '' && creator_)
                                                    heldBy_ = creator_;

                                                if (heldBy_ && applyDate_) {
                                                    if (evnt.evntAppliedBy != heldBy_)
                                                        evnt.evntAppliedBy = heldBy_;

                                                    if (evnt.evntAppliedDate != applyDate_)
                                                        evnt.evntAppliedDate = applyDate_;
                                                }

                                                if (evnt.evntCreator != creator_)
                                                    evnt.evntCreator = creator_;

                                                if (evnt.evntCreated != created_)
                                                    evnt.evntCreated = created_;

                                                if (evnt.evntChanger != changer_)
                                                    evnt.evntChanger = changer_;

                                                if (changed_ && evnt.evntChanged != changed_)
                                                    evnt.evntChanged = changed_;
                                            }
                                        }
                                    }
                            } else {
                                errorMessage.append(flagId_ + " Hold/Permission cannot be found").append("::")
                                return
                            }
                        }
                    }
                }
            }
        }
        LOGGER.warn("logfordebugging 5 before processing dates in ctr use")
        // LFD
        String port_lfd = jsonObj.getOrDefault("port_lfd", null)
        Date lfdDate = port_lfd ? parseDate(port_lfd) : null

        if (ufv.ufvLastFreeDay != lfdDate)
            ufv.ufvLastFreeDay = lfdDate;

        // PTD
        String port_ptd = jsonObj.getOrDefault("port_ptd", null)
        Date ptdDate = port_ptd ? parseDate(port_ptd) : null


        if (ufv.ufvPaidThruDay != ptdDate)
            ufv.ufvPaidThruDay = ptdDate;

        // Guarantee Party
        String guarantor = jsonObj.getOrDefault("customer", null)

        guarantor = (guarantor && guarantor != 'null') ? guarantor : null;
        LineOperator guaranteeParty = null;

        if (guarantor)
            guaranteeParty = findLineOperator(guarantor);

        if ((guaranteeParty != null) && (ufv.ufvGuaranteeParty != guaranteeParty))
            ufv.ufvGuaranteeParty = guaranteeParty;

        // Guarantee Date
        String port_gtd = jsonObj.getOrDefault("port_gtd", null)
        Date guaranteeDate = port_gtd ? parseDate(port_gtd) : null

        if (outTime && guaranteeDate && guaranteeDate.after(parseDate(outTime)))
            guaranteeDate = parseDate(outTime);

        if (ufv.ufvGuaranteeThruDay != guaranteeDate)
            ufv.ufvGuaranteeThruDay = guaranteeDate;

        LOGGER.warn("logfordebugging 5 after processing dates in ctr use")


        if ((eqClass == 'CTR') && (shandId != null)) {
            if (!SpecialStowIgnoreList.contains(shandId)) {
                SpecialStow specialStow = SpecialStow.findOrCreateSpecialStow(shandId);
                if (specialStow == null) {
                    errorMessage.append("Unknown special stow - " + shandId).append("::")
                    return
                }
                unit.unitSpecialStow = specialStow;
            }
        }

        String evnt_count = jsonObj.getOrDefault("evnt_count", null)
        if (!StringUtils.isEmpty(evnt_count)) {
            int eventCount = Integer.valueOf(evnt_count)

            if (eventCount > 0) {
                List<JSONArray> evntList = (List<JSONArray>) jsonObj.getOrDefault("event-list", null)
                JSONArray eventArray = evntList.get(0)
                Date lastMoveEventDate = null;

                for (int i = 0; i < eventArray.size(); i++) {

                    JSONObject evntObj = eventArray.getJSONObject(i)
                    String eventId = evntObj.getOrDefault("event_id", null)
                    eventId = (eventId && eventId != 'null') ? eventId : null;

                    String eventNote = evntObj.getOrDefault("notes", null)
                    eventNote = (eventNote && eventNote != 'null') ? eventNote : null;

                    String qty = evntObj.getOrDefault("quantity_provided", null)
                    def n4Event = EventMap[eventId];
                    if (n4Event) //there is an N4 equivalent to Express event type
                        if (EventType.findEventType(n4Event)?.isBillable() && !MoveKindsMap.containsKey(n4Event))
                            continue
                    Integer quantity = null
                    if (!StringUtils.isEmpty(qty)) {
                        quantity = Integer.parseInt(qty)
                    }
                    String appliedBy_ = evntObj.getOrDefault("performer", null)
                    String evntCreated = evntObj.getOrDefault("created", null)
                    Date created_ = evntCreated ? parseDate(evntCreated) : null
                    String performed = evntObj.getOrDefault("performed", null)
                    Date appliedDate_ = performed ? parseDate(performed) : null
                    LOGGER.warn("logfordebugging Created on " + created_);
                    if (appliedDate_ != null)
                        created_ = appliedDate_
                    String creator_ = evntObj.getOrDefault("creator", null)
                    String changer_ = evntObj.getOrDefault("changer", null)
                    String eventChanged = evntObj.getOrDefault("changed", null)

                    def changed_ = eventChanged ? parseDate(eventChanged) : null;
                    //TODO -- check gkey
                    /*Long gkey_ = (se.@'gkey'.text() && se.@'gkey'.text() != 'null') ? Long.parseLong(se.@'gkey'.text()) : null;

                    def ev = (gkey_ != null ? this.getUnitEventByBatchId(gkey_) : null);*/
                    def ev = null
                    eventId = n4Event ? n4Event : eventId;




                    ev = this.recordMoveEvent(ufv, evntObj, equipmentType, eventId, vesselVisitId, created_);

                    if (ev) {
                        if (lastMoveEventDate == null)
                            lastMoveEventDate = appliedDate_;
                        else if (appliedDate_.after(lastMoveEventDate))
                            lastMoveEventDate = appliedDate_;

                        if (ufv.ufvTimeOfLastMove != lastMoveEventDate)
                            ufv.ufvTimeOfLastMove = lastMoveEventDate;
                    }

                    if (ev == null)
                        ev = this.recordEvent(eventId, eventNote, unit, quantity);

                }
            }
        }


        if (chassis != null) {
            unit.attachCarriage(chassis)
        }


        if (unit.unitAcryId != accessoryId) {
            if (unit.unitAcryId) {
                def ue = unit.getUeInRole(EqUnitRoleEnum.ACCESSORY)

                if (ue)
                    ue.detach("$ue.ueEquipment.eqIdFull : DATA MIGRATION");
            }

            if (accessory)
                unit.attachAccessory(accessory);
        }
        unit.updateDenormalizedFields(false);
        ContextHelper.threadSnxSupressInternalUnitEvents = false;

        LOGGER.warn("logfordebugging 6 before processing bl")
        if (blCount != null && blCount > 0 && !UnitCategoryEnum.EXPORT.equals(cat)) {
            def blm = Roastery.getBean(BillOfLadingManager.BEAN_ID);
            List<JSONArray> blList = (List<JSONArray>) jsonObj.getOrDefault("billofLading-list", null)
            JSONArray blArray = blList.get(0)
            for (int i = 0; i < blCount; i++) {
                JSONObject blObj = blArray.getJSONObject(i)
                String blNumber = blObj.getOrDefault("bl_nbr", null)
                blNumber = (blNumber && blNumber != 'blNumber') ? blNumber : null;
                // blNumber = fixBLNumber(blNumber, lineOperator)
                if (blNumber != null) {

                    List<BillOfLading> list = BillOfLading.findAllBillsOfLading(blNumber);
                    def billOfLading = list != [] ? list[0] : null;
                    if (billOfLading == null) {
                        // if (blNumber != null && unit.getUnitLineOperator() != null && unit.getUnitLineOperator().getBzuScac() != null) {
                        if (blNumber.startsWith(unit.getUnitLineOperator().getBzuScac())) {
                            blNumber = blNumber.substring(unit.getUnitLineOperator().getBzuScac().size())
                        }
                    }
                    List<BillOfLading> blLists = BillOfLading.findAllBillsOfLading(blNumber);
                    billOfLading = blLists != [] ? blLists[0] : null;
                    if (billOfLading == null) {
                        if (unit.getUnitLineOperator().getBzuId().equalsIgnoreCase("MSC") || unit.getUnitLineOperator().getBzuId().equalsIgnoreCase("MED")) {
                            if (blNumber.startsWith("MSCU") || blNumber.startsWith("MEDU")) {
                                blNumber = blNumber.substring(4)
                            }
                        }
                        //errorMessage.append("Bill of Lading not exists : " + blNumber).append("::");
                    }
                    List<BillOfLading> billOfLadingList = BillOfLading.findAllBillsOfLading(blNumber);
                    billOfLading = billOfLadingList != [] ? billOfLadingList[0] : null;
                    if (billOfLading == null) {
                        errorMessage.append("Bill of Lading not exists : " + blNumber).append("::");
                    }
                    if (billOfLading) {
                        Boolean found = false;
                        Set goodsBls = billOfLading.blBlGoodsBls;
                        def blgoodsbl = null;

                        goodsBls.each {
                            goodsBl ->
                                if (goodsBl.blgdsblGoodsBl && goodsBl.blgdsblGoodsBl.gdsUnit && goodsBl.blgdsblGoodsBl.gdsUnit.unitId == unit.unitId) {
                                    found = true;
                                    blgoodsbl = goodsBl;
                                }
                        }

                        if (!found)
                            blm.assignUnitBillOfLading(unit, billOfLading);

                        goodsBls.each {
                            goodsBl ->
                                if (goodsBl.blgdsblGoodsBl && goodsBl.blgdsblGoodsBl.gdsUnit && goodsBl.blgdsblGoodsBl.gdsUnit.unitId == unit.unitId) {
                                    String creator__ = blObj.getOrDefault("creator", null)
                                    String changer__ = blObj.getOrDefault("changer", null)
                                    String holdCreated = blObj.getOrDefault("created", null)
                                    String holdChanged = blObj.getOrDefault("changed", null)

                                    Date created__ = holdCreated ? parseDate(holdCreated) : null
                                    Date changed__ = holdChanged ? parseDate(holdChanged) : null

                                    if (goodsBl.blgdsblChanged != changed__)
                                        goodsBl.blgdsblChanged = new Date();

                                    if (goodsBl.blgdsblChanger != changer__)
                                        goodsBl.blgdsblChanger = changer__;

                                    if (goodsBl.blgdsblCreated != created__)
                                        goodsBl.blgdsblCreated = created__;

                                    if (goodsBl.blgdsblCreator != creator__)
                                        goodsBl.blgdsblCreator = creator__;
                                }
                        }
                    }
                }
            }
        }
        LOGGER.warn("logfordebugging 7 after processing bl")
        if (eqoNumber && unit.unitPrimaryUe.ueDepartureOrderItem == null) {
            if (cat != null && UnitCategoryEnum.EXPORT.equals(cat)) {
                eqoLineId = lineId;
                eqoSubType = 'BOOK';
            }

            if (!['BOOK', 'EDO', 'ELO'].contains(eqoSubType)) {
                errorMessage.append("EQO Sub Type must be BOOK, EDO, or ELO. Current value is " + eqoSubType).append("::")
                return
            }

            if (eqoLineId == null) {
                errorMessage.append("Missing EQO Line Operator").append("::")
            }

            LineOperator eqoLine = LineOperator.findLineOperatorById(eqoLineId);

            if (eqoLine == null) {
                errorMessage.append("Unknown EQO Line Operator " + eqoLineId);
                return
            }

            def eqo = null  // could be booking, EDO, ELO

            switch (eqoSubType) {
                case 'BOOK':
                    CarrierVisit carrierVisit = CarrierVisit.findVesselVisit(ContextHelper.threadFacility, vesselVisitId);
                    eqo = Booking.findBookingByUniquenessCriteria(eqoNumber, eqoLine, carrierVisit);
                    if (eqo == null) {
                        List<Booking> bkgList = Booking.findBookingsByNbr(eqoNumber)
                        if (bkgList != null && bkgList.size() == 1) {
                            eqo = bkgList.get(0)
                        }
                    }
                    break;
                case 'EDO':
                    eqo = EquipmentDeliveryOrder.findEquipmentDeliveryOrder(eqoNumber, eqoLine);
                    break;
                case 'ELO':
                    eqo = EquipmentLoadoutOrder.findEquipmentLoadoutOrder(eqoNumber, eqoLine);
                    break;
            }

            if (eqo == null) {
                errorMessage.append("Cannot find EQO ${eqoNumber}").append("::").append("Vessel visit id ${vesselVisitId}").append("::")
                return
            }

            Set items = eqo.eqboOrderItems;

            if (items == null) {
                errorMessage.append("EQO " + eqoNumber + " of type " + eqoSubType + " has no items - container " + equipmentId).append("::")
                return
            }

            if (items.size() == 0) {
                errorMessage.append("EQO " + eqoNumber + " of type " + eqoSubType + " has zero items - container " + equipmentId).append("::")
            }

            // BI 208893 - For rail containers that are on a booking, if the booking has only one item,
            //             copy the size/type from the booking item to the container, if they're not the same.

            Boolean isRailContainer = false
            if (locType != null) {
                switch (locType) {
                    case "R":
                        isRailContainer = true
                        break
                    case "Y":
                        if ((locId != null) && (position != null) && (group != null)) {
                            if ((locId == "SMT") && ((position == "GRAVE") || (position == "TRI")) && (group == "ICTF"))
                                isRailContainer = true
                        }
                        break
                }
            }

            if (isRailContainer) {
                if (unit.unitEquipment.isEqTypeUpdateAllowed()) {
                    // if container is on booking
                    if (eqo.eqboSubType == EquipmentOrderSubTypeEnum.BOOK) {
                        // if booking only has one item
                        if (items.size() == 1) {
                            def firstItem = items.toList()[0]
                            EquipType itemType = EquipType.findEquipType(firstItem.eqoiEqHeight, firstItem.eqoiEqIsoGroup, firstItem.eqoiEqSize, EquipClassEnum.CONTAINER)
                            // todo check  EquipType itemType = EquipType.findEquipType(items[0].eqoiEqHeight, items[0].eqoiEqIsoGroup, items[0].eqoiEqSize, EquipClassEnum.CONTAINER)
                            // if equipment types don't match, set container's equipment type to item's equipment type
                            if (equipmentType != itemType) {
                                equipmentType = itemType
                                unit.unitEquipment.upgradeEqType(itemType.eqtypId, DataSourceEnum.SNX)
                            }
                        }
                    }
                }
            }
            def newEquipmentTypeId = null
            //  def newEquipmentTypeId = config.getFilterValueForKey("DM_ISO_CODES", equipmentType.eqtypId);

            EquipType newEquipmentType = null;
            if (newEquipmentTypeId) {
                newEquipmentType = EquipType.findEquipType(newEquipmentTypeId);

                if (newEquipmentType == null) {
                    errorMessage.append(" ${newEquipmentTypeId} which maps to ${equipmentType.eqtypId} in EDI Filter does not match an existing Equipment Type in N4");
                }
                equipmentType = newEquipmentType;
            }

            // EquipmentOrder.findMatchingItemRcv does not return null if it doesn't find the item - it throws an exception.
            EquipmentOrderItem eqoi = eqo.findMatchingItemRcv(equipmentType, false, false)

            //unit.assignToOrder(eqoi, equipment)
            unit.unitPrimaryUe.ueDepartureOrderItem = eqoi;
            eqoi.eqboiOrder.incrementTallyRcv(unit);
        }

        return unit;
    }

    public Date parseDate(inDate) {
        if (inDate == null || inDate == '' || inDate == 'null')
            return null

        return inDate ? new java.text.SimpleDateFormat('yyyyMMddHHmmss').parse(inDate) : ''
        //return inDate ? new java.text.SimpleDateFormat('yyyy-MM-dd HH mm ss').parse(inDate) : ''
        //return date ? new java.text.SimpleDateFormat('yyyy MM dd HH:mm:ss').parse(date) : ''
    }

    private String getGeneralReference(String id1, String id2) {
        GeneralReference gref = GeneralReference.findUniqueEntryById(id1, id2);
        return (gref != null ? gref.refValue1 : null);
    }

    def getEquipmentTypeForEquipmentOrder(eqo) {
        if (eqo == null)
            return []

        def items = eqo.eqboOrderItems

        if (items == null)
            return []

        def result = []

        items.each
                {
                    eqoi ->

                        if (eqoi.eqoiSampleEquipType)
                            result.add(eqoi.eqoiSampleEquipType.eqtypId)
                }

        return result
    }


    def applyHoldUnitEquipment(flagTypeId, serviceable, note, referenceId, applyDate, appliedBy) {
        if (serviceable == null) {
            return
        }

        def entityType = serviceable.getLogicalEntityType();

        if (entityType == LogicalEntityEnum.UNIT) {
            if (serviceable.unitVisitState == UnitVisitStateEnum.DEPARTED)
                return

            if (serviceable.unitVisitState == UnitVisitStateEnum.RETIRED)
                return
        }

        def flagTypeObj = FlagType.findFlagType(flagTypeId)

        if (flagTypeObj == null) {
            errorMessage.append("Cannot find hold type " + flagTypeId);
            return
        }

        if (flagTypeObj.flgtypPurpose != FlagPurposeEnum.HOLD) {
            errorMessage.append(flagTypeId + " exists but is not a hold");
            return
        }

        if (flagTypeObj.flgtypAppliesTo != entityType) {
            errorMessage.append("Hold " + flagTypeId + " exists but does not apply to entity type " + entityType.key);
            return
        }

        if (this.hasActiveFlag(serviceable, flagTypeObj))
            return false

        Roastery.getBean(ServicesManager.BEAN_ID).applyHold(flagTypeId, serviceable, referenceId, note ? "DM: $note" : 'Applied by DM', true)

        def flags = flagTypeObj.findMatchingActiveFlags(serviceable, null, null, null, false)

        if (flags != []) {
            flags.get(0).flagAppliedDate = applyDate
            flags.get(0).flagAppliedBy = appliedBy
        }

        return true
    }

    def releaseHoldUnitEquipment(flagTypeId, serviceable, note, referenceId, applyDate, appliedBy) {
        if (serviceable == null) {
            return
        }
        def entityType = serviceable.getLogicalEntityType();

        if (entityType == LogicalEntityEnum.UNIT) {
            if (UnitVisitStateEnum.DEPARTED.equals(serviceable.unitVisitState)) {
                return
            } else if (UnitVisitStateEnum.RETIRED.equals(serviceable.unitVisitState)) {
                return
            }
        }

        def flagTypeObj = FlagType.findFlagType(flagTypeId)

        if (flagTypeObj == null) {
            errorMessage.append("Cannot find hold type " + flagTypeId);
            return
        }

        if (flagTypeObj.flgtypPurpose != FlagPurposeEnum.HOLD) {
            errorMessage.append(flagTypeId + " exists but is not a hold")
            return
        }

        if (flagTypeObj.flgtypAppliesTo != entityType) {
            errorMessage.append(flagTypeId + " exists but does not apply to entity type " + entityType.key);
            return
        }

        if (!(this.hasActiveFlag(serviceable, flagTypeObj)))
            return false

        def flags = flagTypeObj.findMatchingActiveFlags(serviceable, null, null, null, false)
        def note2 = 'Released by DM.'

        if (flags != []) {
            note2 = note2 + "Applied date:" + flags.get(0).flagAppliedDate
            flags.get(0).flagAppliedDate = applyDate
            flags.get(0).flagAppliedBy = appliedBy
        }

        Roastery.getBean(ServicesManager.BEAN_ID).applyPermission(flagTypeId, serviceable, referenceId, note ? "DM: $note . $note2" : "DM: $note2", true)

        return true
    }

    def hasActiveFlag(flagable, flagType) {
        if (flagType == null || flagType == '')
            return false

        def flags = flagType.findMatchingActiveFlags(flagable, null, null, null, true)

        return flags != []
    }

    def getUnitEventByBatchId(batchId) {
        def query = QueryUtils.createDomainQuery(ServicesEntity.EVENT)
                .addDqPredicate(PredicateFactory.eq(ServicesField.EVNT_APPLIED_TO_CLASS, LogicalEntityEnum.UNIT))
                .addDqPredicate(PredicateFactory.eq(ServicesField.EVNT_BILLING_EXTRACT_BATCH_ID, batchId))

        def list = HibernateApi.instance.findEntitiesByDomainQuery(query)

        return list ? list[0] : null
    }

    def recordEvent(eventName, eventNote, servicable, quantity) {
        if (servicable == '') {
            errorMessage.append("recordEvent requires an object to record the event against").append("::")
            return null
        }

        if (eventName == null) {
            errorMessage.append('Event Name cannot be blank').append("::")
            return null
        }

        def sm = Roastery.getBean(ServicesManager.BEAN_ID)

        if (sm == null) {
            errorMessage.append('Cannot create ServicesManager')
            return null
        }

        def eventType = EventType.findOrCreateEventType(eventName, 'Event created by Data Migration', LogicalEntityEnum.UNIT, null)
        //eventType.evnttypeIsBillable = false

        if (eventType == null) {
            errorMessage.append("Cannot find Event Type " + eventName);
            return null
        }

        return sm.recordEvent(eventType, eventNote, quantity, ServiceQuantityUnitEnum.ITEMS, servicable, null)
    }

    private UnitFacilityVisit getUfvCountByEqUseGkey(gkey) {
        def query = QueryUtils.createDomainQuery(InventoryEntity.UNIT_FACILITY_VISIT)
                .addDqPredicate(PredicateFactory.eq(UnitField.UFV_EQ_CLASS, EquipClassEnum.CONTAINER))
                .addDqPredicate(PredicateFactory.eq(UnitField.UFV_FLEX_STRING06, gkey))

        def list = HibernateApi.instance.findEntitiesByDomainQuery(query)

        return list ? list[0] : null;
    }

    private DrayStatusEnum getDrayStatus(drayStatus) {
        switch (drayStatus) {
            case 'F': return DrayStatusEnum.FORWARD;
            case 'I': return DrayStatusEnum.DRAYIN;
            case 'R': return DrayStatusEnum.RETURN;
        }

        errorMessage.append("Cannot map dray status " + drayStatus);
        return null
    }

    private UnitCategoryEnum getCategory(expressCategory) {
        switch (expressCategory) {
            case 'E': return UnitCategoryEnum.EXPORT;
            case 'I': return UnitCategoryEnum.IMPORT;
            case 'M': return UnitCategoryEnum.STORAGE;
            case 'T': return UnitCategoryEnum.THROUGH; //TODO change to TRANSSHIP after view fix for category T
                //BI 208432 Set category to Domestic for 'D'
            case 'D': return UnitCategoryEnum.DOMESTIC;
                //case 'D': return UnitCategoryEnum.THROUGH;
            case 'R': return UnitCategoryEnum.THROUGH;
            case 'S': return UnitCategoryEnum.THROUGH;
        }
        errorMessage.append("Cannot map category " + expressCategory).append("::");
        return null
    }

    private FreightKindEnum getFreightKind(expressStatus) {
        switch (expressStatus) {
            case 'E': return FreightKindEnum.MTY;
            case ['F', 'L']: return FreightKindEnum.FCL;
        }
        errorMessage.append("Cannot map freight kind " + expressStatus).append("::")
        return null
    }

    private LocTypeEnum getLocType(String inLocType) {
        switch (inLocType) {
            case 'T': return LocTypeEnum.TRUCK;
            case 'V': return LocTypeEnum.VESSEL;
            case 'R': return LocTypeEnum.TRAIN;
            case 'Y': return LocTypeEnum.YARD;
        }

        return LocTypeEnum.TRUCK; //default to TRUCK
    }

    def getGenericCarrierVisit(locType) {
        if (locType == '')
            return null

        def mode = this.getLocType(locType)

        return CarrierVisit.findOrCreateGenericCv(ContextHelper.threadComplex, mode)
    }

    private UfvTransitStateEnum getUfvTransitState(inTime, outTime, category, locType, blCount) {
        // Mark all units with an in and out time as Departed, since those will not depart in N4
        if (outTime)
            return UfvTransitStateEnum.S70_DEPARTED;

        // Advised / Inbound
        if (inTime == '' || inTime == null) {

            if (category == 'I' && blCount != null && blCount > 0) {
                return UfvTransitStateEnum.S20_INBOUND;
            } else if ((category == 'E' || category == 'M') && locType == 'R') { //inbound - rail
                return UfvTransitStateEnum.S20_INBOUND;
            } else {
                return UfvTransitStateEnum.S10_ADVISED;
            }

        }
        // In the yard
        if (locType == 'Y')
            return UfvTransitStateEnum.S40_YARD;

        // Load on to Rail
        if (locType == 'R')
            return UfvTransitStateEnum.S60_LOADED;

        // Load on to Rail
        if (locType == 'T')
            return UfvTransitStateEnum.S60_LOADED;

        // Through container
        if (['S', 'R'].contains(category))
            return UfvTransitStateEnum.S60_LOADED;

        if (locType == 'C')
            return UfvTransitStateEnum.S70_DEPARTED;

        if (inTime && locType == 'V' && category == 'E' && (outTime == null || outTime == ''))
            return UfvTransitStateEnum.S70_DEPARTED;


        // Everything else
        errorMessage.append("Dont know how to map transit state inTime=" + inTime + " outTime=" + outTime + " category=" + category + " locType=" + locType)
        return null
    }

    private UnitVisitStateEnum getUfvVisitState(visitState) {
        switch (visitState) {
            case UfvTransitStateEnum.S10_ADVISED: return UnitVisitStateEnum.ADVISED;
            case UfvTransitStateEnum.S60_LOADED: return UnitVisitStateEnum.ACTIVE;
            case UfvTransitStateEnum.S70_DEPARTED: return UnitVisitStateEnum.DEPARTED;
            case UfvTransitStateEnum.S40_YARD: return UnitVisitStateEnum.ACTIVE;
            case UfvTransitStateEnum.S20_INBOUND: return UnitVisitStateEnum.ACTIVE;
        }

        errorMessage.append("Dont know how to map visit state " + visitState).append("::")
        return null
    }

    private VentUnitEnum getVentilationUnit(ventilationUnit) {
        if (ventilationUnit == '' || ventilationUnit == null || ventilationUnit == ' ')
            return null;

        switch (ventilationUnit) {
            case '%': return VentUnitEnum.PERCENTAGE;
            case 'CMH': return VentUnitEnum.CUBIC_M_HOUR;
            case 'CFM': return VentUnitEnum.CUBIC_FT_MIN;
        }

        errorMessage.append("Dont know how to map ventilation unit " + ventilationUnit).append("::")
        return null
    }

    private Double convertWeight(grossWeight, unit) {
        def w = Double.valueOf(grossWeight)

        switch (unit) {
            case 'KG': return w.round();
            case 'KT': return (w * 1000000).round();
            case 'LB': return (w * 0.4535923699997481).round();
            case 'MT': return (w * 1000).round();
        }

        return w;
    }

    def recordMoveEvent(ufv, se, equipmentType, eventId, vesselVisitId, created) {
        def moveKind = MoveKindsMap[eventId]
        log("recording move event for " + moveKind);
        if (moveKind == null)
            return null

        def moveEvent = null
        def moveInfo = MoveInfoBean.createDefaultMoveInfoBean(moveKind, created)
        moveEvent = MoveEvent.recordMoveEvent(ufv, ufv.getUfvArrivePosition(), ufv.getUfvLastKnownPosition(), ufv.ufvActualIbCv, moveInfo, MoveEvent.moveKind2EventEnum(moveKind))
        //       }

        return moveEvent;
    }

    def getCarrierVisitForMoveEvent(shipId, voyageNumber, vesselVisitId) {
        if ((shipId == '' || shipId == null) && (voyageNumber == '' || voyageNumber == null))
            return null

        def id = shipId

        if (voyageNumber.size() > 2)
            id += voyageNumber.substring(0, 2)
        else
            id += voyageNumber

        // use vesselVisitId
        return CarrierVisit.findOrCreateVesselVisit(ContextHelper.threadFacility, vesselVisitId)
    }

    private CarrierVisit getCarrierVisit(String locType, String visitId, String locId, String vesVisitId) {
        CarrierVisit cv = null;
        switch (this.getLocType(locType)) {
            case LocTypeEnum.TRUCK:
                cv = locId == "TRUCK" ? CarrierVisit.getGenericTruckVisit(ContextHelper.threadComplex) : null;
                if (!cv)
                    cv = locId ? CarrierVisit.findOrCreateTruckVisitForEdi(ContextHelper.threadFacility, locId) : null;
                break;
            case LocTypeEnum.VESSEL:
                cv = vesVisitId ? CarrierVisit.findOrCreateVesselVisit(ContextHelper.threadFacility, vesVisitId) : null;
                break;
            case LocTypeEnum.TRAIN:
                cv = visitId ? CarrierVisit.findOrCreateTrainVisit(ContextHelper.threadFacility, visitId) : null;
                LOGGER.debug(" getCarrierVisitcv Train" + cv)
                break;
            default:
                log("getCarrierVisit: Unsupported locType = " + locType);
                break;
        }
        if (cv)
            HibernateApi.instance.saveOrUpdate(cv);
        return cv;
    }

    def createLocPosition(locType, locId, posId, equipmentType, cv, shipId, voyageNumber) {
        def mode = this.getLocType(locType)

        switch (mode) {
            case LocTypeEnum.TRUCK:
                def tv = this.getCarrierVisit(locType, null, locId, null);
                return LocPosition.createTruckPosition(tv, posId, null);
            case LocTypeEnum.VESSEL:
                return LocPosition.createVesselPosition(cv, posId, null);
            case LocTypeEnum.TRAIN:
                def ttvv = CarrierVisit.findCarrierVisit(ContextHelper.threadFacility, LocTypeEnum.TRAIN, voyageNumber)
                def _locId = locId ? (locId.size() > 3 ? locId.substring(0, 3) : locId) : locId
                return LocPosition.createTrainPosition(ttvv, posId, _locId, null)
            case LocTypeEnum.YARD:
                return LocPosition.createYardPosition(ContextHelper.threadYard, posId, null, equipmentType.eqtypBasicLength, false)
        }

        return null;
    }

    void markEventsNonBillable(unit) {
        def events = em.getEventHistory(null, unit)
        events.each {
            event ->
                event.evntEventType.evnttypeIsBillable = false
        }
    }

    def createContainer(id, equipmentType) {
        def container = new Container()

        container.equipmentIdFields = id
        container.eqEquipType = equipmentType
        container.updateEquipTypeProperties(equipmentType)
        container.eqDataSource = ContextHelper.threadDataSource

        HibernateApi.instance.save(container)

        return container
    }

    private void checkUnitEquipmentState(Unit unit) {
        switch (unit.unitEquipmentStates.size()) {
            case 0:
                unit.unitEquipmentStates.add(EquipmentState.createEquipmentStateFromUe(unit));
                break;
            case 1:
                if (unit.unitEquipmentStates[0].eqsLastPosLocType == LocTypeEnum.UNKNOWN)
                    unit.unitEquipmentStates.add(EquipmentState.createEquipmentStateFromUe(unit));
                break;
            default:
                log("DEBUG checkUnitEquipmentState : " + unit.unitId + " has " + unit.unitEquipmentStates.size().toString() + " equipment states");
                break;
        }
    }


    private void attachDamages(Unit unit, def jsonObj) {
        String dmg_count = jsonObj.getOrDefault("dmg_count", null)
        if (!StringUtils.isEmpty(dmg_count)) {
            Integer dmgCount = Integer.parseInt(dmg_count);
            //Container level severity if it's  V - make all items MINOR severity
            String eqDmgSeverity = jsonObj.getOrDefault("damaged", null)

            Boolean isVDamage = (eqDmgSeverity == "V")
            if (dmgCount != null && dmgCount > 0) {
                List<JSONArray> dmgList = (List<JSONArray>) jsonObj.getOrDefault("damages-list", null)
                JSONArray dmgArray = dmgList.get(0)
                UnitEquipDamages damages = new UnitEquipDamages();
                for (int i = 0; i < dmgCount; i++) {
                    def dmgObj = dmgArray.getJSONObject(i)
                    String dmgId = dmgObj.getOrDefault("damage_id", null)

                    String dmgDesc = dmgObj.getOrDefault("description", null)

                    String dmgArea = dmgObj.getOrDefault("area", null)
                    if (dmgArea == null) dmgArea = "BX";
                    if (dmgArea.equalsIgnoreCase("CNTREQ"))
                        dmgArea = "1CNTREQ"
                    String dmgSeverity = dmgObj.getOrDefault("severity", null)

                    EquipDamageType dmgType = EquipDamageType.findEquipDamageType(dmgId, EquipClassEnum.CONTAINER);
                    if (dmgType == null) {
                        errorMessage.append("Damage type could not be found for id=" + dmgId);
                        return
                    }
                    EqComponent dmgComponent = EqComponent.findEqComponent(dmgArea.toUpperCase(), EquipClassEnum.CONTAINER);
                    if (dmgComponent == null) {
                        errorMessage.append("Damage component could not be found for area=" + dmgArea);
                        return
                    }
                    String dmgReportedDate = dmgObj.getOrDefault("reported", null)
                    Date dmgReported = !StringUtils.isEmpty(dmgReportedDate) ? parseDate(dmgReportedDate) : null
                    UnitEquipDamageItem dmgItem = damages.addDamageItem(dmgType, dmgComponent,
                            isVDamage ? EqDamageSeverityEnum.MINOR : getDmgSeverity(dmgSeverity),
                            dmgReported, null);
                    if (dmgItem == null) {
                        errorMessage.append("Failed to add damage item");
                        return
                    }
                    String dmgCreated = dmgObj.getOrDefault("created", null)

                    dmgItem.dmgitemCreated = !StringUtils.isEmpty(dmgReportedDate) ? parseDate(dmgReportedDate) : null

                    dmgItem.dmgitemCreator = dmgObj.getOrDefault("creator", null)

                    dmgItem.dmgitemDescription = dmgObj.getOrDefault("notes", null)

                    // Set VDamage field to 'Y'
                    if (!isVDamage)
                        isVDamage = dmgSeverity == 'V'
                }
                unit.attachDamages(unit.unitEquipment, damages);
            } else if (eqDmgSeverity) {
                //default damage type
                String defaultDamageTypeId = "000"
                //Default damage component
                String defaultDamageComponentId = "000"
                UnitEquipDamages damages = new UnitEquipDamages();

                EquipDamageType dmgType = EquipDamageType.findEquipDamageType(defaultDamageTypeId, EquipClassEnum.CONTAINER);
                if (dmgType == null) {
                    errorMessage.append("Damage type could not be found for id=" + defaultDamageTypeId);
                    return
                }
                EqComponent dmgComponent = EqComponent.findEqComponent(defaultDamageComponentId, EquipClassEnum.CONTAINER);
                if (dmgComponent == null) {
                    errorMessage.append("Damage component could not be found for area=" + defaultDamageComponentId);
                    return
                }
                UnitEquipDamageItem dmgItem = damages.addDamageItem(dmgType, dmgComponent,
                        getDmgSeverity(eqDmgSeverity),
                        new Date(), null);
                if (dmgItem == null) {
                    errorMessage.append("Failed to add damage item type: $dmgType, component: $dmgComponent, severity: $eqDmgSeverity");
                    return
                }
                unit.attachDamages(unit.unitEquipment, damages);
            }

            if (isVDamage)
                unit.unitFlexString01 = 'Y'
        }
    }

    private String convertSealNbr(def docSealNbr) {
        String SealNbr
        SealNbr = docSealNbr ? docSealNbr.toString() : null;
        SealNbr = (SealNbr && SealNbr != 'null' && SealNbr != '0') ? SealNbr : null;
        return SealNbr
    }

    private LineOperator findLineOperator(String id) {
        DomainQuery query = QueryUtils.createDomainQuery("LineOperator").addDqPredicate(PredicateFactory.eq(ArgoRefField.BZU_ID, id));
        LineOperator line = (LineOperator) HibernateApi.getInstance().getUniqueEntityByDomainQuery(query);
        if (line == null) {
            query = QueryUtils.createDomainQuery("LineOperator").addDqPredicate(PredicateFactory.eq(ArgoRefField.BZU_SCAC, id));
            line = (LineOperator) HibernateApi.getInstance().getUniqueEntityByDomainQuery(query);
        }
        return line;
    }

    def EventMap = ['YARD SHIFT': 'UNIT_YARD_SHIFT',
                    'CORRECTION': 'UNIT_POSITION_CORRECTION',
                    'EMPTY IN'  : 'UNIT_RECEIVE',
                    'GATE OUT'  : 'UNIT_DELIVER',
                    'GATE_IN'   : 'UNIT_RECEIVE',
                    'DISCHARGE' : 'UNIT_DISCH',
                    'LOAD'      : 'UNIT_LOAD',
                    'FULL OUT'  : 'UNIT_DELIVER',
                    'FULL IN'   : 'UNIT_RECEIVE',
                    'EMPTY OUT' : 'UNIT_DELIVER',
                    'RAIL LOAD' : 'UNIT_RAMP',
                    'RAILUNLOAD': 'UNIT_DERAMP',
                    'DAMAGE'    : 'UNIT_DAMAGED',
                    'REPAIR'    : 'UNIT_REPAIRED',
                    'FB/O'      : 'UNIT_FLIP_BAD_ORDER',
                    'FOWN'      : 'UNIT_FLIP_OWN',
                    'ROLL'      : 'UNIT_ROLL_LATER_VESSEL_EVENT',
                    'FBIT'      : 'UNIT_FLIP_BIT',
                    'FWRG'      : 'UNIT_FLIP_WRONG_CHASSIS',
                    'FLIP'      : 'UNIT_FLIP_OWN',
                    'FRFR'      : 'UNIT_FLIP_REEFER',
                    'DRAY OFF'  : 'RETURN TO SHIPPER',
                    'RENUMBER'  : 'UNIT_RENUMBER',
                    'CHG OWNER' : 'UNIT_OPERATOR_CHANGED',
                    'OFFHIRE'   : 'UNIT_OFFHIRE',
                    'ONHIRE'    : 'UNIT_ONHIRE',
                    'BUNDLE'    : 'UNIT_BUNDLE',
                    'PREADVISE' : 'UNIT_PREADVISE',
                    'UNBUNDLE'  : 'UNIT_UNBUNDLE',
                    'DRAYINFEE' : 'PAC_FEE_DRAY',
                    'TLGTFEE'   : 'PAC_FEE_TAILGATE',
                    'TLGTFEE2'  : 'PAC_FEE_TAILGATE_SHIPSIDE',
                    'USCSHIP'   : 'PAC_FEE_XRAY_SHIPSIDE',
                    'USCDRAY'   : 'PAC_FEE_DRAY_TO_XRAY',
                    'USCGFEE'   : 'PAC_FEE_COAST_GUARD']

    def MoveKindsMap = ['UNIT_YARD_SHIFT'   : WiMoveKindEnum.YardShift,
                        'UNIT_YARD_MOVE'    : WiMoveKindEnum.YardMove,
                        'UNIT_RECEIVE'      : WiMoveKindEnum.Receival,
                        'UNIT_DELIVER'      : WiMoveKindEnum.Delivery,
                        'UNIT_DISCH'        : WiMoveKindEnum.VeslDisch,
                        'UNIT_RAMP'         : WiMoveKindEnum.RailLoad,
                        'UNIT_LOAD'         : WiMoveKindEnum.VeslLoad,
                        'UNIT_DERAMP'       : WiMoveKindEnum.RailDisch,
                        'EXPRESS_TRUCK2YARD': WiMoveKindEnum.YardMove]
    List<String> SpecialStowIgnoreList = ['APV', 'GEN', 'MTY', 'NEE', 'NO ', 'NOR', 'PAR', 'TLG']

    private EqDamageSeverityEnum getDmgSeverity(String expressSeverity) {
        switch (expressSeverity) {
            case "":
            case "M":
            case "V":
                return EqDamageSeverityEnum.MINOR
                break
            default:
                return EqDamageSeverityEnum.MAJOR
        }
    }

    List<Serializable> getMessagesToProcess() {
        DomainQuery domainQuery = QueryUtils.createDomainQuery(ArgoIntegrationEntity.INTEGRATION_SERVICE_MESSAGE)
        domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_ENTITY_CLASS, LogicalEntityEnum.UNIT));
        domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_USER_STRING3, "false"))
        domainQuery.addDqOrdering(Ordering.desc(ArgoIntegrationField.ISM_SEQ_NBR))
        domainQuery.addDqPredicate(PredicateFactory.isNull(ArgoIntegrationField.ISM_USER_STRING4))
        domainQuery.addDqPredicate(PredicateFactory.isNull(ArgoIntegrationField.ISM_INTEGRATION_SERVICE))

        //    domainQuery.addDqPredicate(PredicateFactory.in(ArgoIntegrationField.ISM_USER_STRING1, ['TEMU2851181']))
                .setDqMaxResults(3000)
        return HibernateApi.getInstance().findPrimaryKeysByDomainQuery(domainQuery)
    }
    Map<String, String> yardEmptyLineSubst = ['APL': 'CMA',
                                              'MED': 'MSC'];
    Map<String, String> _ventReqSubst = ['???' : '',
                                         'null': '',
                                         'CLOS': '0',
                                         'Z'   : '0']

    private static final String UnLoc = "UNLOCCODE";
}