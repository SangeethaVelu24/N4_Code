import com.navis.argo.ArgoPropertyKeys
import com.navis.argo.ContextHelper
import com.navis.argo.business.atoms.FreightKindEnum
import com.navis.argo.business.atoms.LocTypeEnum
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.atoms.WiMoveKindEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.reference.EquipType
import com.navis.argo.business.reference.Equipment
import com.navis.argo.util.XmlUtil
import com.navis.cargo.business.model.BillOfLading
import com.navis.cargo.business.model.GoodsBl
import com.navis.external.argo.AbstractArgoCustomWSHandler
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.UserContext
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.query.common.api.QueryResult
import com.navis.framework.util.ValueObject
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageCollector
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.moves.MoveEvent
import com.navis.inventory.business.units.EqBaseOrder
import com.navis.inventory.business.units.GoodsBase
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.road.RoadEntity
import com.navis.road.RoadField
import com.navis.road.business.atoms.TranStatusEnum
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import com.navis.vessel.business.schedule.VesselVisitDetails
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.jdom.Element
import org.jdom.Namespace

import java.text.SimpleDateFormat

/**
 * @Author: weservetech.com; Date: 28-Nov-2024
 *
 *  Requirements: WBCT - 336 - Equipment History for all users
 *
 * @Inclusion Location: Incorporated as a code extension of the type
 *
 *  Load Code Extension to N4:
 *  1. Go to Administration --> System --> Code Extensions
 *  2. Click Add (+)
 *  3. Enter the values as below:
 *     Code Extension Name: PAEquipmentHistoryHandler
 *     Code Extension Type:WS_ARGO_CUSTOM_HANDLER
 *     Groovy Code: Copy and paste the contents of groovy code.
 *  4. Click Save button
 *
 *  S.No    Modified Date   Modified By     Jira        Description
 *  1.      28-Nov-2024      Sangeetha V     WBCT:336    Added additional attribute field which is retrieved from Truck transaction.
 */
class PAEquipmentHistoryHandler extends AbstractArgoCustomWSHandler {
    private static final Logger logger = Logger.getLogger(PAEquipmentHistoryHandler.class)

    @Override
    void execute(final UserContext uc,
                 final MessageCollector mc,
                 final Element inElement, final Element inOutEResponse, final Long wsLogKey) {


        Element rootEleHistory = inElement.getChild(CUSTOM_EQUIPMENT_MOVE_HISTORY)
        Element historyResponse = new Element(EQUIPMENT_MOVE_HISTORY)
        String freightKind = EMPTY
        String grossWeight = EMPTY
        String tareWeight = EMPTY
        String sealNo = EMPTY
        String bookingNbr = EMPTY
        String bolNbr = EMPTY
        String voyIn = EMPTY
        String voyOut = EMPTY
        if (rootEleHistory) {
            Element parameterEle = rootEleHistory.getChild(PARAMETER)

            if (parameterEle) {
                String eqId = parameterEle.getAttributeValue(EQUIPMENT_ID)

                if (StringUtils.isNotEmpty(eqId)) {
                    String[] unitNbrs = eqId.split(",")*.trim()
                    boolean isEmpty = false

                    if (unitNbrs.size() > 50) {
                        getMessageCollector().appendMessage(MessageLevel.INFO, PropertyKeyFactory.valueOf("COUNT_LIMIT_EXCEEDED"), null, null)
                    }
                    DomainQuery dq = QueryUtils.createDomainQuery("MoveEvent")
                            .addDqPredicate(PredicateFactory.in(MetafieldIdFactory.valueOf("mveUfv.ufvUnit.unitId"), unitNbrs?.take(50)))
                            .addDqField(MetafieldIdFactory.valueOf("evntGkey"))
                            .addDqField(MetafieldIdFactory.valueOf("mveUfv.ufvUnit.unitId"))

                    QueryResult result = HibernateApi.getInstance().findValuesByDomainQuery(dq)
                    List vaoList = result.getRetrievedResults()
                    Iterator vaoIter;
                    Map map = new HashMap()

                    MetafieldId unitId = MetafieldIdFactory.valueOf("mveUfv.ufvUnit.unitId")
                    for (vaoIter = vaoList.iterator(); vaoIter.hasNext();) {
                        ValueObject vao = (ValueObject) vaoIter.next();
                        map.put(vao.getEntityPrimaryKey(), vao.getFieldValue(unitId))
                    }

                    for (String eqNbr : unitNbrs?.take(50)) {
                        def moveEventGkeys = []
                        map.findAll { i -> i.value == eqNbr }.each { moveEventGkeys << it.getKey() }
                        Element EquipElemt = new Element(EQUIPMENT)

                        Equipment eq = Equipment.findEquipment(eqNbr)
                        EquipElemt.setAttribute(NBR, eqNbr)
                        if (eq != null) {
                            EquipElemt.setAttribute(TYPE, eq.getEqClass().getKey())
                            EquipType type = eq.getEqEquipType()
                            String size = type.getEqtypNominalLength().getKey().replaceAll("NOM", EMPTY)
                            String height = type.getEqtypNominalHeight().getKey().replaceAll("NOM", EMPTY)
                            String iso = type.getEqtypIsoGroup().getKey()

                            if (height.equalsIgnoreCase("NA")) {
                                height = EMPTY
                            }
                            String szTypeHt = size + iso + height

                            if (moveEventGkeys != null && !moveEventGkeys.isEmpty()) {
                                for (Serializable mveGkey : moveEventGkeys) {
                                    if (mveGkey != null) {
                                        MoveEvent mveEvnt = MoveEvent.hydrate(mveGkey)
                                        Element moveEle = new Element(MOVE)
                                        Unit unit = mveEvnt.getMveUfv().getUfvUnit()

                                        String pol = (unit.getUnitRouting() != null && unit.getUnitRouting().getRtgPOL() != null) ? unit.getUnitRouting().getRtgPOL().getPointId() : EMPTY
                                        String pod = (unit.getUnitRouting() != null && unit.getUnitRouting().getRtgPOD1() != null) ? unit.getUnitRouting().getRtgPOD1().getPointId() : EMPTY
                                        String evntTransitType = (mveEvnt.getMveCarrier() != null && mveEvnt.getMveCarrier().getLocType() != null) ? mveEvnt.getMveCarrier().getLocType().getKey() : EMPTY
                                        String trkScac = null
                                        if (LocTypeEnum.TRUCK.equals(mveEvnt?.getMveCarrier()?.getCvCarrierMode())) {
                                            TruckVisitDetails tvdetails = TruckVisitDetails.resolveFromCv(mveEvnt.getMveCarrier())
                                            trkScac = tvdetails?.getTvdtlsTrkCompany()?.getBzuScac()
                                        }
                                        bookingNbr = getBkgNbr(unit) != null ? getBkgNbr(unit) : EMPTY
                                        bolNbr = getBlNbr(unit) != null ? getBlNbr(unit) : EMPTY
                                        sealNo = unit.getUnitSealNbr1() != null ? unit.getUnitSealNbr1() : (unit.getUnitSealNbr2() != null ? unit.getUnitSealNbr2() : EMPTY)
                                        switch (unit.getUnitFreightKind()) {
                                            case FreightKindEnum.MTY:
                                                freightKind = "E"
                                                break;
                                            case FreightKindEnum.FCL:
                                            case FreightKindEnum.LCL:
                                                freightKind = "F"
                                                break;
                                            default:
                                                freightKind = EMPTY
                                        }
                                        grossWeight = unit.getUnitGoodsAndCtrWtKg().toString() != null ? unit.getUnitGoodsAndCtrWtKg().toString() : EMPTY
                                        tareWeight = unit?.getUnitEquipment()?.getEqTareWeightKg()?.toString() != null ? unit?.getUnitEquipment()?.getEqTareWeightKg()?.toString() : EMPTY
                                        XmlUtil.setOptionalAttribute(moveEle, EVENT, mveEvnt.getEventTypeId(), (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, DATE, mveEvnt.getEvntAppliedDate().toString(), (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, LINE, unit.getUnitLineOperator().getBzuId(), (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, STATUS, unit.getUnitCategory().getKey(), (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, SZTPHT, szTypeHt, (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, CT, mveEvnt.getMveFromPosition().getPosLocType().getKey(), (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, CARRIER, mveEvnt.getMveFromPosition().getPosLocId(), (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, TRANSIT_TYPE, (evntTransitType != null ? (evntTransitType.equals("VESSEL") ? "V" : (evntTransitType.equals("TRAIN") ? "R" : evntTransitType.equals("TRUCK") ? "T" : EMPTY)) : EMPTY), (Namespace) null)
                                        XmlUtil.setOptionalAttribute(moveEle, POSITION, mveEvnt?.getMveFromPosition()?.getPosSlot() != null ? mveEvnt.getMveFromPosition().getPosSlot() : EMPTY, (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, CARGO_VVC, mveEvnt.getMveCarrier() != null ? mveEvnt.getMveCarrier().getCvId() : EMPTY, (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, POL, pol, (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, POD, pod, (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, TRUCK_SCAC, trkScac != null ? trkScac : EMPTY, (Namespace) null)

                                        String vesselCode = EMPTY

                                        CarrierVisit cv = retrieveCarrierVisit(unit)
                                        if (cv != null) {
                                            VesselVisitDetails vvd = VesselVisitDetails.resolveVvdFromCv(cv)
                                            if (vvd != null) {
                                                vesselCode = vvd.getVesselId() != null ? vvd.getVesselId() : EMPTY

                                                voyIn = vvd.getVvdIbVygNbr() != null ? vvd.getVvdIbVygNbr() : EMPTY
                                                voyOut = vvd.getVvdObVygNbr() != null ? vvd.getVvdObVygNbr() : EMPTY

                                            }
                                        }
                                        XmlUtil.setOptionalAttribute(moveEle, VESSEL_CODE, vesselCode, (Namespace) null);
                                        XmlUtil.setOptionalAttribute(moveEle, BOOKING_NO, bookingNbr, (Namespace) null)
                                        XmlUtil.setOptionalAttribute(moveEle, BL_NBR, bolNbr, (Namespace) null)
                                        XmlUtil.setOptionalAttribute(moveEle, FREIGHT_KIND, freightKind, (Namespace) null)
                                        XmlUtil.setOptionalAttribute(moveEle, GRS_WEIGHT, grossWeight, (Namespace) null)
                                        XmlUtil.setOptionalAttribute(moveEle, TARE_WEIGHT, tareWeight, (Namespace) null)
                                        XmlUtil.setOptionalAttribute(moveEle, SEAL_NO, sealNo, (Namespace) null)
                                        XmlUtil.setOptionalAttribute(moveEle, VOY_IN, voyIn, (Namespace) null)
                                        XmlUtil.setOptionalAttribute(moveEle, VOY_OUT, voyOut, (Namespace) null)
                                        validateGateTransaction(mveEvnt, moveEle)
                                        EquipElemt.addContent(moveEle)
                                    }
                                }

                            } else if (moveEventGkeys.isEmpty()) {
                                isEmpty = true
                            }
                        }
                        if (eq == null || isEmpty) {
                            if (eq == null) {
                                EquipElemt.setAttribute(TYPE, EMPTY)
                            }
                            Element moveEle = new Element(MOVE)
                            moveEle.addContent("No data to display")
                            EquipElemt.addContent(moveEle)
                        }
                        historyResponse.addContent(EquipElemt)
                    }
                    inOutEResponse.addContent(historyResponse)
                } else if (StringUtils.isEmpty(eqId)) {
                    mc.appendMessage(MessageLevel.WARNING, ArgoPropertyKeys.VALIDATION_REQUIRED_FIELD, null, "equipment ID is required")
                }

            }
        }
    }


    void validateGateTransaction(MoveEvent mveEvnt, Element moveEle) {
        if (mveEvnt != null) {
            String gateTranNbr = EMPTY
            String gateTranDate = EMPTY

            String chassisNbr = EMPTY
            String refIn = EMPTY
            String refOut = EMPTY
            Date createdDate = null
            CarrierVisit cv = null
            UnitFacilityVisit ufv = mveEvnt.getMveUfv()
            if (ufv != null) {
                Unit unit = ufv.getUfvUnit()

                List<TruckTransaction> truckTransactionList = retrieveGateTransaction(unit)
                if (truckTransactionList != null && truckTransactionList.size() > 0) {
                    for (TruckTransaction truckTransaction : truckTransactionList) {
                        Unit tranUnit = truckTransaction.getTranUnit()
                        if (tranUnit != null) {
                            if (tranUnit.getUnitGkey() == unit.getUnitGkey()) {

                                if (WiMoveKindEnum.Delivery.equals(mveEvnt.getMveMoveKind()) && truckTransaction.isDelivery()) {
                                    createdDate = truckTransaction.getTranCreated() != null ? truckTransaction.getTranCreated() : null
                                    gateTranNbr = truckTransaction.getTranNbr() != null ? String.valueOf(truckTransaction.getTranNbr()) : EMPTY
                                    chassisNbr = truckTransaction.getTranChsNbr() != null ? truckTransaction.getTranChsNbr() : EMPTY
                                } else if (WiMoveKindEnum.Receival.equals(mveEvnt.getMveMoveKind()) && truckTransaction.isReceival()) {
                                    createdDate = truckTransaction.getTranCreated() != null ? truckTransaction.getTranCreated() : null
                                    gateTranNbr = truckTransaction.getTranNbr() != null ? String.valueOf(truckTransaction.getTranNbr()) : EMPTY
                                    chassisNbr = truckTransaction.getTranChsNbr() != null ? truckTransaction.getTranChsNbr() : EMPTY
                                }

                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYMMdd")
                                String date = createdDate != null ? simpleDateFormat.format(createdDate) : null
                                gateTranDate = createdDate != null ? String.valueOf(createdDate) : EMPTY

                                String refNo = date != null ? date + gateTranNbr : EMPTY
                                if (WiMoveKindEnum.Delivery.equals(mveEvnt.getMveMoveKind())) {
                                    refOut = refNo != null ? refNo : EMPTY
                                } else if (WiMoveKindEnum.Receival.equals(mveEvnt.getMveMoveKind())) {
                                    refIn = refNo != null ? refNo : EMPTY
                                }
                            }
                        }
                    }
                    XmlUtil.setOptionalAttribute(moveEle, GATE_TRAN_DATE, gateTranDate, (Namespace) null)
                    XmlUtil.setOptionalAttribute(moveEle, GATE_TRAN_NBR, gateTranNbr, (Namespace) null)
                    XmlUtil.setOptionalAttribute(moveEle, CHASSIS_NBR, chassisNbr, (Namespace) null)
                    XmlUtil.setOptionalAttribute(moveEle, REF_IN, refIn, (Namespace) null)
                    XmlUtil.setOptionalAttribute(moveEle, REF_OUT, refOut, (Namespace) null)
                }

            }
        }
    }

    private CarrierVisit retrieveCarrierVisit(Unit unit) {
        CarrierVisit cv = null
        if (unit != null) {
            UnitFacilityVisit ufv = unit.getUnitActiveUfvNowActive() != null ? unit.getUnitActiveUfvNowActive() : unit.getUfvForFacilityNewest(ContextHelper.getThreadFacility())
            if (ufv != null) {
                if (UnitCategoryEnum.IMPORT.equals(unit.getUnitCategory())) {
                    cv = ufv.getUfvActualIbCv()
                } else if (UnitCategoryEnum.EXPORT.equals(unit.getUnitCategory())) {
                    cv = ufv.getUfvActualObCv()
                }
            }
        }
        return cv
    }

    String getBkgNbr(Unit unit) {
        if (unit != null) {
            String bkgNbr = null
            if (unit.getDepartureOrder() != null) {
                EqBaseOrder baseOrder = unit.getDepartureOrder()
                if (baseOrder != null) {
                    bkgNbr = baseOrder.getEqboNbr()
                }
            }
            return bkgNbr
        }
    }

    String getBlNbr(Unit unit) {
        if (unit) {
            GoodsBase goodsBase = unit.getUnitGoods()
            GoodsBl goodsBl = GoodsBl.resolveGoodsBlFromGoodsBase(goodsBase)
            Set<BillOfLading> billOfLadingSet
            String bls = ""
            if (goodsBl != null) {
                billOfLadingSet = goodsBl.getGdsblBillsOfLading()
                for (BillOfLading blNbr : billOfLadingSet) {
                    bls = blNbr.getBlNbr()
                }
            }
            return bls
        }

    }

    List<TruckTransaction> retrieveGateTransaction(Unit unit) {
        if (unit != null) {
            DomainQuery query = QueryUtils.createDomainQuery(RoadEntity.TRUCK_TRANSACTION)
                    .addDqPredicate(PredicateFactory.eq(RoadField.TRAN_CTR_NBR, unit.getUnitId()))
                    .addDqPredicate(PredicateFactory.in(RoadField.TRAN_STATUS, [TranStatusEnum.OK, TranStatusEnum.COMPLETE]))
                    .addDqPredicate(PredicateFactory.isNotNull(RoadField.TRAN_CTR_NBR))
            return (List<TruckTransaction>) HibernateApi.getInstance().findEntitiesByDomainQuery(query)
        }
    }

    private static final String MOVE = "move"
    private static final String TYPE = "type"
    private static final String NBR = "Nbr"
    private static final String PARAMETER = "parameter"
    private static final String EQUIPMENT = "equipment"
    private static final String EQUIPMENT_ID = "equipment-id"
    private static final String EQUIPMENT_MOVE_HISTORY = "equipment-move-history"
    private static final String CUSTOM_EQUIPMENT_MOVE_HISTORY = "custom-equipment-move-history"
    private static final String EVENT = "event"
    private static final String DATE = "date"
    private static final String LINE = "line"
    private static final String STATUS = "status"
    private static final String SZTPHT = "szTpHt"
    private static final String CT = "ct"
    private static final String CARRIER = "carrier"
    private static final String TRANSIT_TYPE = "transit-type"
    private static final String POSITION = "position"
    private static final String CARGO_VVC = "cargo-vvc"
    private static final String POL = "pol"
    private static final String POD = "pod"
    private static final String TRUCK_SCAC = "truck-scac"
    private static final String VESSEL_CODE = "vessel-code"
    private static final String GATE_TRAN_DATE = "gatetran-date"
    private static final String GATE_TRAN_NBR = "gatetran-no"
    private static final String VOY_IN = "voy-in"
    private static final String VOY_OUT = "voy-out"
    private static final String BOOKING_NO = "booking-no"
    private static final String BL_NBR = "billing-no"
    private static final String CHASSIS_NBR = "chassis-no"
    private static final String FREIGHT_KIND = "fe"
    private static final String GRS_WEIGHT = "grs-weight"
    private static final String TARE_WEIGHT = "tare-weight"
    private static final String SEAL_NO = "seal-no"
    private static final String REF_IN = "ref-in"
    private static final String REF_OUT = "ref-out"
    private static final String EMPTY = ""

}