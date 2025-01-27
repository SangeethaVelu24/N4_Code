import com.navis.argo.business.model.LocPosition
import com.navis.argo.business.model.Yard
import com.navis.argo.business.reference.Equipment
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.inventory.business.api.UnitField
import com.navis.inventory.business.atoms.UfvTransitStateEnum
import com.navis.inventory.business.units.Unit
import com.navis.inventory.business.units.UnitFacilityVisit
import com.navis.spatial.business.model.block.AbstractBlock
import com.navis.yard.business.model.AbstractYardBlock
import com.navis.yard.business.model.YardBinModel

import static com.navis.argo.ContextHelper.getThreadYard

class WBCTPositionCorrectionScriptRunner2 {
//b2r2c2, b2r2c3
    def yardblockMap = ['S8', 'T8', 'F0', 'V2', 'W2', 'U2', 'S2', 'N2', 'P1', 'P2', 'N1', 'R2', 'Q2', 'X2', 'T2', 'A0', 'C0', 'D0']

    private static
    final MetafieldId ufv_pos_slot = MetafieldIdFactory.getCompoundMetafieldId(UnitField.UFV_LAST_KNOWN_POSITION, UnitField.POS_SLOT);

    String execute() {
        StringBuilder builder = new StringBuilder()

        String unitId = "OTPU6359853"
        String blk = "S8"
       //  for (String blk : yardblockMap) {
             DomainQuery domainQuery = QueryUtils.createDomainQuery("UnitFacilityVisit")
                     .addDqPredicate(PredicateFactory.like(ufv_pos_slot, "${blk}%"))
                     .addDqPredicate(PredicateFactory.eq(UnitField.UFV_TRANSIT_STATE, UfvTransitStateEnum.S40_YARD))
                    // .addDqPredicate(PredicateFactory.eq(UnitField.UFV_UNIT_ID, unitId))
             //     .setDqMaxResults(10)


             List<UnitFacilityVisit> ufvList = (List<UnitFacilityVisit>) HibernateApi.getInstance().findEntitiesByDomainQuery(domainQuery)
             if (ufvList != null && !ufvList.isEmpty()) {
                 for (UnitFacilityVisit ufv : ufvList) {
                     // UnitFacilityVisit ufv = ufvList.get(0)
                     Unit unit = ufv.getUfvUnit()

                     Equipment ctr = unit.getUnitEquipment()

                     LocPosition position = ufv.getUfvLastKnownPosition()
                     builder.append("UNIT ID " + unit.getUnitId()).append("\n")
                     builder.append("position " + position.getPosSlot()).append("\n")
                     builder.append("position length " + position.getPosSlot().length()).append("\n")
                     builder.append("position.getBlockName() " + position.getBlockName()).append("\n")
                     String blockCode = position.getPosSlot() != null ? position.getPosSlot().substring(0, 2).concat("W") : null
                     builder.append("block code " + blockCode).append("\n")
                     if (position.getPosSlot().length() >= 6
                             && yardblockMap.contains(position.getPosSlot().substring(0, 2))) {
//&& position.getBlockName() == null

                         Yard thisYard = getThreadYard();
                         YardBinModel yardModel =
                                 (YardBinModel) HibernateApi.getInstance().downcast(thisYard.getYrdBinModel(), YardBinModel.class)
                         AbstractBlock ayBlock = AbstractYardBlock.findYardBlockByCode(yardModel.getPrimaryKey(), blockCode)
                         builder.append("ayBlock " + ayBlock?.getAbnName()).append("\n")

                         if (ayBlock != null && ayBlock.getAbnName() != null) {
                             LocPosition blockPos = LocPosition.createYardPosition(threadYard, ayBlock.getAbnName(), null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                             AbstractYardBlock finalAyBlock = (blockPos.getPosBin() != null) ? (AbstractYardBlock) HibernateApi.getInstance().downcast(blockPos.getPosBin(), AbstractYardBlock.class) : null;
                             builder.append("finalAyBlock.getAyblkLabelSchemeHost() " + finalAyBlock.getAyblkLabelSchemeHost()).append("\n")
                             builder.append("position.getPosSlot().length() " + position.getPosSlot().length()).append("\n")
                             builder.append("position.getPosSlot() " + position.getPosSlot()).append("\n")
                             // if (finalAyBlock.getAyblkLabelSchemeHost().equalsIgnoreCase('B3R2C2')) {
                             if (finalAyBlock.getAyblkLabelSchemeHost().equalsIgnoreCase('B2R2C3') || finalAyBlock.getAyblkLabelSchemeHost().equalsIgnoreCase('B2R2C2')) {
                                 String posSlot = position.getPosSlot()
                                 builder.append("posSlot " + posSlot).append("\n")

                                 if (position.getPosSlot().length() == 10) {
                                     StringBuilder sb = new StringBuilder(position.getPosSlot());

                                     builder.append("length is 10 ").append("\n")
                                     if (sb.charAt(2) == sb.charAt(3)) {
                                         builder.append("same char at 2 and 3").append("\n")
                                         sb.replace(2, 5, '1').replace(sb.length() - 3, sb.length() - 2, "")
                                         builder.append("sb " + sb).append("\n")

                                         posSlot = sb.toString()
                                     }

                                     LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                     builder.append("final block pos " + finalblockPos).append("\n")
                                     // ufv.setUfvLastKnownPosition(finalblockPos)
                                 }

                                 if (position.getPosSlot().length() == 8) {
                                     StringBuilder sb = new StringBuilder(position.getPosSlot());
                                     // s8 1 849 1 ---- Todo
                                     // S80149.1

                                     if (sb.charAt(2) == sb.charAt(4)) {
                                         sb.deleteCharAt(2)
                                         posSlot = sb.toString()
                                         builder.append("pos slot 2 and 4 are equal.. " + posSlot).append("\n")
                                         LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                         builder.append("final block pos " + finalblockPos).append("\n")
                                         // ufv.setUfvLastKnownPosition(finalblockPos)
                                         continue
                                     }

                                     if (sb.charAt(1) == sb.charAt(3)) {

                                         sb.insert(position.getPosSlot().length() - 6, '0');
                                         sb.deleteCharAt(4)

                                         posSlot = sb.toString()
                                         builder.append("pos slot " + posSlot).append("\n")
                                         // posSlot = sb.toString()
                                     }

                                     if (sb.charAt(2).toString().equals("0")) {
                                         sb.deleteCharAt(2)
                                         posSlot = sb.toString()
                                         builder.append("sb char 2 is 0 " + posSlot).append("\n")
                                     }
                                     if (sb.charAt(1) == sb.charAt(2)) {
                                         sb.deleteCharAt(2)
                                         posSlot = sb.toString()
                                         builder.append("pos slot when character 1 and 2 are equal " + posSlot).append("\n")
                                     }
                                     LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                     builder.append("final block pos " + finalblockPos).append("\n")
                                    // ufv.setUfvLastKnownPosition(finalblockPos)
                                 }

                             } else if (finalAyBlock.getAyblkLabelSchemeHost().equalsIgnoreCase('B3R2C2') || finalAyBlock.getAyblkLabelSchemeHost().equalsIgnoreCase('B3R2C1')) {
                                 String posSlot = position.getPosSlot()
                                 //cc9 906 n .1 --cc906n1
                                 if (position.getPosSlot().length() == 9) {
                                     StringBuilder sb = new StringBuilder(position.getPosSlot());
                                     if (sb.charAt(2) == sb.charAt(3)) {

                                         // sb.insert(position.getPosSlot().length() -6, '0');
                                         sb.deleteCharAt(3)

                                         posSlot = sb.toString()

                                     }

                                 }
                                 if (position.getPosSlot().length() == 8) {
                                     StringBuilder sb = new StringBuilder(position.getPosSlot());

                                     if (sb.charAt(2).toString().equals("1")) {
                                         sb.replace(2, 4, "")
                                         sb.insert(position.getPosSlot().length() - 6, 'WA');
                                         posSlot = sb.toString()
                                         builder.append("sb char 2 is 1 " + posSlot).append("\n")
                                     }
                                 }
                                 LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                 builder.append("final block pos " + finalblockPos).append("\n")
                                // ufv.setUfvLastKnownPosition(finalblockPos)

                             } else if (finalAyBlock.getAyblkLabelSchemeHost().equalsIgnoreCase('B3R1C2') || (finalAyBlock.getAyblkLabelSchemeHost().equalsIgnoreCase('B3R1C3'))) {
                                 String posSlot = position.getPosSlot()
                                 StringBuilder sb = new StringBuilder(position.getPosSlot());
                                 if (position.getPosSlot().length() == 8) {
                                     if (sb.charAt(2).toString().equals("1")) {
                                         sb.replace(2, 4, "")
                                         sb.insert(position.getPosSlot().length() - 6, 'WA');
                                         posSlot = sb.toString()
                                         builder.append("sb char 2 is 1 " + posSlot).append("\n")
                                     }
                                 }

                                 if (position.getPosSlot().length() == 10){
                                    if (sb.charAt(4) == '8'){
                                        sb.deleteCharAt(4)
                                        posSlot = sb.toString()
                                        builder.append("sb char 4 is 8 " + posSlot).append("\n")

                                    }
                                 }
                                 LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                 builder.append("final block pos " + finalblockPos).append("\n")
                                  ufv.setUfvLastKnownPosition(finalblockPos)
                             }
                         }
                     }
                 }
             }
       //  }

        return builder.toString()
    }

}




