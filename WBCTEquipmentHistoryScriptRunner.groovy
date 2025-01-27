package WBCT

import com.navis.argo.business.atoms.FreightKindEnum
import com.navis.argo.business.atoms.WiMoveKindEnum
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.Disjunction
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.Junction
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.query.common.api.QueryResult
import com.navis.framework.util.ValueObject
import com.navis.inventory.InvField
import com.navis.inventory.business.api.InventoryCompoundField
import com.navis.inventory.business.moves.MoveEvent
import com.navis.inventory.business.units.Unit
import com.navis.road.RoadEntity
import com.navis.road.RoadField
import com.navis.road.business.model.TruckTransaction
import com.navis.vessel.business.schedule.VesselVisitDetails
import org.jdom.Element

import java.text.SimpleDateFormat

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

class WBCTEquipmentHistoryScriptRunner {

    String execute() {

        StringBuilder sb = new StringBuilder()



        Junction ctrNbrConjunction = PredicateFactory.conjunction()
                .add(PredicateFactory.eq(RoadField.TRAN_CTR_NBR, "TLLU5380888"))

        Junction ctrNbrAssignedConjunction = PredicateFactory.conjunction()
                .add(PredicateFactory.eq(RoadField.TRAN_CTR_NBR_ASSIGNED, "TLLU5380888"))

        Disjunction disjunction = new Disjunction();
        disjunction.add(ctrNbrConjunction)
        disjunction.add(ctrNbrAssignedConjunction)

        DomainQuery query = QueryUtils.createDomainQuery(RoadEntity.TRUCK_TRANSACTION)
               // .addDqPredicate(PredicateFactory.eq(RoadField.TRAN_CTR_NBR_ASSIGNED,"TLLU5308888"))
                .addDqPredicate(disjunction)


        sb.append("query " + query).append("\n")

        List<TruckTransaction> truckTransactionList = HibernateApi.getInstance().findEntitiesByDomainQuery(query)

        sb.append("truck transaction list " + truckTransactionList).append("\n")
/*
        String unitIds = "OCGU8019200"

        DomainQuery dq = QueryUtils.createDomainQuery("MoveEvent")
                .addDqPredicate(PredicateFactory.in(MetafieldIdFactory.valueOf("mveUfv.ufvUnit.unitId"), unitIds))
                .addDqField(MetafieldIdFactory.valueOf("evntGkey"))
                .addDqField(MetafieldIdFactory.valueOf("mveUfv.ufvUnit.unitId"))

        QueryResult result = HibernateApi.getInstance().findValuesByDomainQuery(dq)

        sb.append("result " + result).append("\n")
        List vaoList = result.getRetrievedResults()
        Iterator vaoIter;
        Map map = new HashMap()

        MetafieldId unitId = MetafieldIdFactory.valueOf("mveUfv.ufvUnit.unitId")
        for (vaoIter = vaoList.iterator(); vaoIter.hasNext();) {
            ValueObject vao = (ValueObject) vaoIter.next();
            map.put(vao.getEntityPrimaryKey(), vao.getFieldValue(unitId))
        }
        sb.append("map " + map).append("\n")
        //for (String eqNbr : unitIds?.take(50)) {
        //  sb.append("eq nbr " + eqNbr).append("\n")
        def moveEventGkeys = []
        Unit unit = null
        MoveEvent mveEvnt = null
        map.findAll { i -> i.value == unitIds }.each { moveEventGkeys << it.getKey() }
        if (moveEventGkeys != null && !moveEventGkeys.isEmpty()) {
            for (Serializable mveGkey : moveEventGkeys) {
                sb.append("mve gkey " + mveGkey).append("\n")
                if (mveGkey != null) {
                    mveEvnt = MoveEvent.hydrate(mveGkey)
                    unit = mveEvnt.getMveUfv().getUfvUnit()
                    sb.append("mv event " + mveEvnt.getEventTypeId()).append("\n")
                    sb.append("unit " + unit).append("\n")


                    DomainQuery query = QueryUtils.createDomainQuery(RoadEntity.TRUCK_TRANSACTION)
                            .addDqPredicate(PredicateFactory.eq(RoadField.TRAN_CTR_NBR_ASSIGNED, unit?.getUnitId()))

                    List<TruckTransaction> truckTransactionList = (List<TruckTransaction>) HibernateApi.getInstance().findEntitiesByDomainQuery(query)

                    sb.append("query " + query).append("\n")

                    sb.append("transaction list " + truckTransactionList).append("\n")
                    String gateTranNbr = ""
                    String gateTranDate = ""
                    String voyIn = ""
                    String voyOut = ""
                    String bookingNbr = ""
                    String bolNbr = ""
                    String chassisNbr = ""
                    String freightKind = ""
                    String grossWeight = ""
                    String tareWeight = ""
                    String sealNo = ""
                    String refIn = ""
                    String refOut = ""

                    if (truckTransactionList != null && truckTransactionList.size() > 0) {
                        for (TruckTransaction truckTransaction : truckTransactionList) {

                            gateTranDate = truckTransaction.getTranCreated() != null ? truckTransaction.getTranCreated() : ""
                            gateTranNbr = truckTransaction.getTranNbr() != null ? String.valueOf(truckTransaction.getTranNbr()) : ""

                            if (truckTransaction.getTranCarrierVisit() != null) {
                                VesselVisitDetails vvd = VesselVisitDetails.resolveVvdFromCv(truckTransaction.getTranCarrierVisit())

                                if (vvd != null) {
                                    voyIn = vvd.getCarrierIbVoyNbrOrTrainId() != null ? vvd.getCarrierIbVoyNbrOrTrainId() : ""
                                    voyOut = vvd.getCarrierObVoyNbrOrTrainId() != null ? vvd.getCarrierObVoyNbrOrTrainId() : ""
                                }
                            }
                            bookingNbr = truckTransaction.getTranEqoNbr() != null ? truckTransaction.getTranEqoNbr() : ""
                            bolNbr = truckTransaction.getTranBlNbr() != null ? truckTransaction.getTranBlNbr() : ""
                            chassisNbr = truckTransaction.getTranChsNbr() != null ? truckTransaction.getTranChsNbr() : ""
                            switch (truckTransaction.getTranCtrFreightKind()) {
                                case FreightKindEnum.MTY:
                                    freightKind = "E"
                                    break;
                                case FreightKindEnum.FCL:
                                    freightKind = "F"
                                    break
                                case FreightKindEnum.LCL:
                                    freightKind = "F"
                                    break;
                                default:
                                    freightKind = ""
                            }

                            grossWeight = truckTransaction.getTranCtrGrossWeight() != null ? String.valueOf(truckTransaction.getTranCtrGrossWeight()) : ""

                            tareWeight = truckTransaction.getTranCtrTareWeight() != null ? String.valueOf(truckTransaction.getTranCtrTareWeight()) : ""

                            sealNo = truckTransaction.getTranSealNbr1() != null ? truckTransaction.getTranSealNbr1() : (truckTransaction.getTranSealNbr2() != null ? truckTransaction.getTranSealNbr2() : "")

                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyddMM")
                            String date = simpleDateFormat.format(truckTransaction.getTranCreated())

                            String refNo = date != null ? date + gateTranNbr : ""

                            if (WiMoveKindEnum.Delivery.equals(mveEvnt.getMveMoveKind())) {
                                refOut = refNo != null ? refNo : ""
                            } else if (WiMoveKindEnum.Receival.equals(mveEvnt.getMveMoveKind())) {
                                refIn = refNo != null ? refNo : ""
                            }
                            sb.append("ref no " + refNo).append("\n")
                            sb.append("date " + date).append("\n")
                            sb.append("gateTranDate " + gateTranDate).append("\n")
                            sb.append("gateTranNbr " + gateTranNbr).append("\n")
                            sb.append("voyIn " + voyIn).append("\n")
                            sb.append("voyOut " + voyOut).append("\n")
                            sb.append("bookingNbr " + bookingNbr).append("\n")
                            sb.append("bolNbr " + bolNbr).append("\n")
                            sb.append("chassisNbr " + chassisNbr).append("\n")
                            sb.append("freightKind " + freightKind).append("\n")
                            sb.append("grossWeight " + grossWeight).append("\n")
                            sb.append("tareWeight " + tareWeight).append("\n")
                            sb.append("sealNo " + sealNo).append("\n")
                            sb.append("refIn " + refIn).append("\n")
                            sb.append("refOut " + refOut).append("\n")
                        }
                    }

                }
            }
        }*/
        return sb.toString()

    }
    public static final MetafieldId TRAN_UFV_ID = MetafieldIdFactory.getCompoundMetafieldId(RoadField.TRAN_UNIT, InvField.UNIT_ID);

    // }

}
