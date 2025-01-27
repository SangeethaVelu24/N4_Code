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

class WBCTCorrectUnitPosition1 {
//todo bb1 is missing


    def yardblockMap = ['WW6', 'RLB', 'DD1', 'FF1', 'BB1', 'QR4', 'NN1', 'QR5', 'QR7', 'RY0', 'BB3']


    //def yardblockMap = ['RR6','CC9','FLW','KK3','AA8','AA7','HH2','KK4','GG1','KK2']
    //def yardblockMap = ['KK1','ZZ4','JJ2','JJ1','FF2','BB2','LL1']
    //def yardblockMap = ['MM1','ZZ2','QQ6','BB7','GG9','HH9','MM9','HH8','GG8','TF6']
    //def yardblockMap = ['HH3','BRD','QQ4','NN5','JJ3','RY1','LL2','DD2']
    //def yardblockMap = ['AA9','MM3','GG2','RY2','NN6','PP3','NN4','HH1','PP4','MM2']
    //def yardblockMap = ['TF5','LL3','PP5','NN3','ZZ3','PP6','MNR','TT5']
    //def yardblockMap = ['DD9','FF7','WW5','TT8','FF9','SS5','UU6','EE7','UU5','EE9']
    //def yardblockMap = ['MM8','GG7','PP8','HH7','FF8','NN8','MM7','QQ5','PP7','NN7']
    //def yardblockMap = ['EE8','RR5','CC8','SS6','DD8','BB8','DD7','CC7','BB9','TT6']


   // def yardblockMap = ['BRG', 'CBP', 'FLP', 'HL100', 'HL102', 'HL126', 'HL127', 'HL131', 'MR1', 'MR2', 'NII', 'TF4', 'ZI3', 'ZJ3', 'ZK3', 'ZL3']

    ///def yardblockMap = ['FF8','HH7','PP8','GG7','MM8','EE9','UU5','RR6','EE7','UU6','SS5','FF9','TT8','WW5','FF7','DD9','TT5']
    ///def yardblockMap = ['MNR','PP6','ZJ3','ZZ3','NN3','PP5','LL3','ZI3','TF5','MM2','PP4','HH1','NN4','PP3','NN6','RY2','GG2']
    ///def yardblockMap = ['MM3','AA9','DD2','LL2','RY1','JJ3','NN5','QQ4','GG8','HH8','MM9','HH9','GG9','BB7','QQ6','ZZ2','MM1']
    /// def yardblockMap = ['LL1','ZL3','BB2','FF2','ZK3','JJ1','JJ2','ZZ4','KK1','NII','KK2','GG1','KK4','NN2','HH2','AA7','AA8','KK3']

    private static
    final MetafieldId ufv_pos_slot = MetafieldIdFactory.getCompoundMetafieldId(UnitField.UFV_LAST_KNOWN_POSITION, UnitField.POS_SLOT);

    String execute() {
        StringBuilder builder = new StringBuilder()

        String unitId = "YMLU7015747"
        String blk = "PP6"
        Yard thisYard = null
        YardBinModel yardModel = null
        AbstractBlock ayBlock = null
        // for (String blk : yardblockMap) {
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
                builder.append("position slot " + position.getPosSlot()).append("\n")
                builder.append("position slot substring " + position.getPosSlot().substring(0, 3)).append("\n")
                builder.append("position length " + position.getPosSlot().length()).append("\n")
                builder.append("position.getBlockName() " + position.getBlockName()).append("\n")
                if (position.getPosSlot().length() >= 6
                        && yardblockMap.contains(position.getPosSlot().substring(0, 3)) || wheeledblockMap.contains(position.getPosSlot().substring(0, 3))) {
//&& position.getBlockName() == null
                    if (position.getPosSlot().startsWith("BRG")) {
                        //BRDG21.35.1 -> BRG11351
                        String posSlot = position.getPosSlot().replace("BRG1", "BRGWA")
                        builder.append("pos slot " + posSlot).append("\n")
                        builder.append("pos slot length " + posSlot.length()).append("\n")
                        if (posSlot.length() == 10) {
                            StringBuilder sb = new StringBuilder(posSlot);
                            sb.deleteCharAt(5)
                            // sb.replace(5, 6, "")
                            builder.append("sb " + sb).append("\n")
                            posSlot = sb.toString()

                        }
                        LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                        builder.append("final block pos " + finalblockPos).append("\n")
                        // ufv.setUfvLastKnownPosition(finalblockPos)
                        continue
                    }

                    StringBuilder st = new StringBuilder(position.getPosSlot());
                    builder.append("st char at 3 " + st.charAt(3)).append("\n")

                    thisYard = getThreadYard();
                    yardModel = (YardBinModel) HibernateApi.getInstance().downcast(thisYard.getYrdBinModel(), YardBinModel.class)

                    if (position.getPosSlot().startsWith("HL1")) {
                        thisYard = getThreadYard();
                        yardModel = (YardBinModel) HibernateApi.getInstance().downcast(thisYard.getYrdBinModel(), YardBinModel.class)
                        ayBlock = AbstractYardBlock.findYardBlockByCode(yardModel.getPrimaryKey(), position.getPosSlot().substring(0, 5).concat('W'))
                        builder.append("ayBlock " + ayBlock?.getAbnName()).append("\n")
                    }

                    if (st.charAt(3) == 'A' || st.charAt(3) == 'B' || wheeledblockMap.contains(position.getPosSlot().substring(0, 3))) {
                        ayBlock = AbstractYardBlock.findYardBlockByCode(yardModel.getPrimaryKey(), position.getPosSlot().substring(0, 3).concat('W')) != null ?
                                AbstractYardBlock.findYardBlockByCode(yardModel.getPrimaryKey(), position.getPosSlot().substring(0, 3).concat('W')) : null
                    } else {
                        ayBlock = AbstractYardBlock.findYardBlockByCode(yardModel.getPrimaryKey(), position.getPosSlot().substring(0, 3)) != null ?
                                AbstractYardBlock.findYardBlockByCode(yardModel.getPrimaryKey(), position.getPosSlot().substring(0, 3)) :
                                (AbstractYardBlock.findYardBlockByCode(yardModel.getPrimaryKey(), position.getPosSlot().substring(0, 4)) != null ?
                                        AbstractYardBlock.findYardBlockByCode(yardModel.getPrimaryKey(), position.getPosSlot().substring(0, 4)) : null)
                    }


                    builder.append("ayblock " + ayBlock).append("\n")
                    builder.append("ayBlock.getAbnName " + ayBlock?.getAbnName()).append("\n")


                    if (ayBlock != null && ayBlock.getAbnName() != null) {
                        LocPosition blockPos = LocPosition.createYardPosition(threadYard, ayBlock.getAbnName(), null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                        AbstractYardBlock finalAyBlock = (blockPos.getPosBin() != null) ? (AbstractYardBlock) HibernateApi.getInstance().downcast(blockPos.getPosBin(), AbstractYardBlock.class) : null;
                        builder.append("finalAyBlock.getAyblkLabelSchemeHost() " + finalAyBlock.getAyblkLabelSchemeHost()).append("\n")
                        builder.append("position.getPosSlot().length() " + position.getPosSlot().length()).append("\n")
                        builder.append("position.getPosSlot() " + position.getPosSlot()).append("\n")
                        // if (finalAyBlock.getAyblkLabelSchemeHost().equalsIgnoreCase('B3R2C2')) {
                        builder.append("final ay block " + finalAyBlock).append("\n")
                        if (finalAyBlock != null && finalAyBlock.getAyblkLabelSchemeHost() == null) {
                            builder.append("final ay block host is null").append("\n")
                            String posSlot = position.getPosSlot()
                            builder.append("posSlot " + posSlot).append("\n")
                            if (position.getPosSlot().length() == 10) {
                                StringBuilder sb = new StringBuilder(position.getPosSlot());
                                // s8 1 849 1 ---- Todo
                                // S80149.1

                                sb.replace(5, position.getPosSlot().length(), "W")
                                posSlot = sb.toString()
                                builder.append("sb after replace " + sb).append("\n")
                                LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                builder.append("final block pos " + finalblockPos).append("\n")
                                // ufv.setUfvLastKnownPosition(finalblockPos)
                            }
                        }

                        if (finalAyBlock != null && finalAyBlock?.getAyblkLabelSchemeHost() != null && finalAyBlock?.getAyblkLabelSchemeHost()?.equalsIgnoreCase('B2R2C3') || finalAyBlock?.getAyblkLabelSchemeHost()?.equalsIgnoreCase('B2R2C2')) {
                            String posSlot = position.getPosSlot()
                            if (position.getPosSlot().length() == 8) {
                                StringBuilder sb = new StringBuilder(position.getPosSlot());
                                // s8 1 849 1 ---- Todo
                                // S80149.1
                                if (sb.charAt(1) == sb.charAt(3)) {

                                    sb.insert(position.getPosSlot().length() - 6, '0');
                                    sb.deleteCharAt(4)

                                    posSlot = sb.toString()

                                    // posSlot = sb.toString()
                                }
                                LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                // ufv.setUfvLastKnownPosition(finalblockPos)
                            }

                        } else if (finalAyBlock?.getAyblkLabelSchemeHost()?.equalsIgnoreCase('B3R2C2') || finalAyBlock?.getAyblkLabelSchemeHost()?.equalsIgnoreCase('B3R2C1')) {
                            String posSlot = position.getPosSlot()
                            builder.append("posSlot " + posSlot).append("\n")
                            builder.append("posSlot length " + posSlot.length()).append("\n")
                            //cc9 906 n .1 as cc9 06 n.1
                            if (position.getPosSlot().length() == 11) {
                                StringBuilder sb = new StringBuilder(position.getPosSlot());
                                if (posSlot.startsWith("RLB")) {
                                    sb.replace(3, 6, "")
                                    //  sb.deleteCharAt(position.getPosSlot().length() - 4)
                                    //  sb.replace((position.getPosSlot().length() - 4),(position.getPosSlot().length() - 3),"")
                                    posSlot = sb.toString()
                                    builder.append("pos slot " + posSlot).append("\n")
                                }

                                if (posSlot.startsWith("TF") && 'M' == sb.charAt(3)) {
                                    sb.replace(3, 6, "")
                                    posSlot = sb.toString()
                                    builder.append("pos slot " + posSlot).append("\n")
                                }
                                builder.append("sb.charAt(3) " + sb.charAt(3)).append("\n")
                                builder.append("sb.charAt(4) " + sb.charAt(4)).append("\n")
                                if (posSlot.startsWith("QR4") && sb.charAt(3) == sb.charAt(4)) {
                                    sb.replace(3, 6, "")
                                    posSlot = sb.toString()
                                    builder.append("pos slot " + posSlot).append("\n")
                                }

                                /*   LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                   builder.append("finalblockPos " + finalblockPos).append("\n")*/
                                // ufv.setUfvLastKnownPosition(finalblockPos)
                            }

                            if (position.getPosSlot().length() == 9) {
                                StringBuilder sb = new StringBuilder(position.getPosSlot());
                                if (sb.charAt(2) == sb.charAt(3)) {

                                    // sb.insert(position.getPosSlot().length() -6, '0');
                                    sb.deleteCharAt(3)

                                    posSlot = sb.toString()
                                    builder.append("sb pos slot " + posSlot).append("\n")
                                    // posSlot = sb.toString()
                                }
                                /* LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                 builder.append("finalblockPos " + finalblockPos).append("\n")*/
                                // ufv.setUfvLastKnownPosition(finalblockPos)
                            } /*else if (position.getPosSlot().length() == 10) {
                                    // builder.append("inside 10")
                                    //BB1B1158.1 in N4 or BB1A1111.1 in n4
                                    // KK1B 1 123 1 as KK1 1 23 1
                                    StringBuilder sb = new StringBuilder(position.getPosSlot());
                                    if (sb.charAt(3) == 'B' || sb.charAt(3) == 'A') {

                                        sb = sb.deleteCharAt(3) // KK11123.1
                                        // builder.append("inside 10 a" +sb.toString())
                                    }
                                    if (sb.charAt(2) == sb.charAt(4)) {

                                        // sb.insert(position.getPosSlot().length() -6, '0');
                                        sb.deleteCharAt(4) // KK1123.1

                                        posSlot = sb.toString()
                                        //builder.append("inside 10 posSlot" +posSlot)
                                        // posSlot = sb.toString()
                                        builder.append("pos slot " + posSlot).append("\n")
                                    }
                                    LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                    builder.append("finalblockPos " + finalblockPos).append("\n")
                                    // ufv.setUfvLastKnownPosition(finalblockPos)
                                }*/
                            if (position.getPosSlot().length() == 10) { // KK3
                                StringBuilder sb = new StringBuilder(position.getPosSlot());
                                builder.append("sb " + sb).append("\n")
                                if (posSlot.startsWith("MNR")) {
                                    builder.append("MNR ").append("\n")
                                    if (sb.charAt(3) == sb.charAt(4)) {
                                        sb.deleteCharAt(3)
                                        posSlot = sb.toString()
                                        builder.append("pos slot " + posSlot).append("\n")
                                    }
                                }

                                if (sb.charAt(3).isLetter()) { // BB1B11.2.11 -> BB121B1
                                    builder.append("3th character is letter").append("\n")
                                    sb.insert(position.getPosSlot().length() - 2, sb.charAt(3));
                                    sb.replace(3, 5, "").replace(sb.length() - 2, sb.length() - 1, "")

                                    posSlot = sb.toString()
                                    builder.append("pos slot " + posSlot).append("\n")
                                }

                                if (sb.charAt(2) == sb.charAt(3)) {
                                    sb.deleteCharAt(3)
                                    posSlot = sb.toString()
                                    builder.append("pos slot " + posSlot).append("\n")
                                    builder.append("sb char at of 3 " + sb.charAt(sb.length() - 4)).append("\n")
                                    builder.append("sb char at of 4 " + sb.charAt(sb.length() - 3)).append("\n")
                                    if (sb.charAt(sb.length() - 4) == sb.charAt(sb.length() - 3)) {
                                        sb.deleteCharAt(sb.length() - 4)
                                        posSlot = sb.toString()
                                    }
                                }
                                builder.append("pos slot " + posSlot).append("\n")
                                /*LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                builder.append("final block pos " + finalblockPos)*/
                                //  ufv.setUfvLastKnownPosition(finalblockPos)
                            }

                            if (position.getPosSlot().length() == 7) { //TF4
                                StringBuilder sb = new StringBuilder(position.getPosSlot());
                                builder.append("sb length 7 " + sb.getAt(3)).append("\n")
                                sb.replace(3, 5, "")
                                posSlot = sb.toString()
                                builder.append("posSlot " + posSlot)
                                /*    LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                    builder.append("final block pos " + finalblockPos)*/
                                // ufv.setUfvLastKnownPosition(finalblockPos)
                            }

                            //zz458I.5 as zz458I5
                            if (position.getPosSlot().length() == 8) {
                                StringBuilder sb = new StringBuilder(position.getPosSlot());
                                builder.append("sb length 8 " + sb).append("\n")
                                sb.insert(position.getPosSlot().length() - 5, '0');
                                sb.replace(7, 8, "")

                                builder.append("after replace " + sb).append("\n")
                                posSlot = sb.toString()
                                /*   LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                   builder.append("final block pos " + finalblockPos)*/
                                //  ufv.setUfvLastKnownPosition(finalblockPos)
                            }

                            //TF51510.1 AS TF505151

                            if (position.getPosSlot().length() == 9) { //TF5 block
                                StringBuilder sb = new StringBuilder(position.getPosSlot());
                                builder.append("sb length 9 " + sb).append("\n")
                                if (sb.charAt(2) == sb.charAt(4)) {

                                    // sb.insert(position.getPosSlot().length() - 6, '0');
                                    //  sb.replace(7, 10, "")
                                    sb.deleteCharAt(4)
                                    builder.append("sb size " + sb.size()).append("\n")
                                    posSlot = sb.toString()
                                    builder.append("pos slot " + posSlot).append("\n")
                                    // ufvPosSlot = sb.toString()
                                }
                                /*  LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                                  builder.append("final block pos " + finalblockPos)
                                //  ufv.setUfvLastKnownPosition(finalblockPos)*/
                                builder.append("update successfully").append("\n")
                            }
                            LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                            builder.append("final block pos " + finalblockPos)
                            // ufv.setUfvLastKnownPosition(finalblockPos)

                        } else if (finalAyBlock?.getAyblkLabelSchemeHost()?.equalsIgnoreCase('B4R1C2') || finalAyBlock?.getAyblkLabelSchemeHost()?.equalsIgnoreCase('B4R2C1')) {
                            String posSlot = position.getPosSlot()
                            builder.append("posSlot " + posSlot).append("\n")
                            builder.append("posSlot length " + posSlot.length()).append("\n")
                            StringBuilder sb = new StringBuilder(position.getPosSlot());

                            if (position.getPosSlot().length() == 10) {

                                if (posSlot.startsWith("FLP") && sb.charAt(3) == 'D') {
                                    sb.deleteCharAt(4)
                                    posSlot = sb.toString()
                                    builder.append("pos slot " + posSlot).append("\n")
                                }

                                if (sb.charAt(3) == 'A') {
                                    sb.insert(position.getPosSlot().length() - 7, 'W');
                                    sb.replace(5, 7, "")
                                    posSlot = sb.toString()
                                    builder.append("pos slot " + posSlot).append("\n")
                                }

                                sb.insert(position.getPosSlot().length() - 7, 'W');
                                //  sb.deleteCharAt(position.getPosSlot().length() - 3)
                                if (sb.charAt(5) == sb.charAt(6)) {
                                    sb.replace(5, 7, "")
                                    posSlot = sb.toString()
                                    builder.append("pos slot " + posSlot).append("\n")
                                }

                            }

                            if (sb.charAt(2) == sb.charAt(4)) {
                                sb.replace(2, 4, "")
                                sb.insert(position.getPosSlot().length() - 6, 'WA');
                                posSlot = sb.toString()
                                builder.append("pos slot " + posSlot).append("\n")
                            }
                            if (position.getPosSlot().length() == 8) {
                                sb.insert(position.getPosSlot().length() - 5, 'WA');
                                sb.deleteCharAt(position.getPosSlot().length() - 3)
                                posSlot = sb.toString()
                                builder.append("pos slot " + posSlot).append("\n")
                            }
                            if (position.getPosSlot().length() == 6) {
                                sb.insert(position.getPosSlot().length() - 3, 'WA');
                                sb.deleteCharAt(position.getPosSlot().length() - 1)
                                posSlot = sb.toString()
                                builder.append("pos slot " + posSlot).append("\n")
                            }


                            LocPosition finalblockPos = LocPosition.createYardPosition(threadYard, posSlot, null, ctr.getEqEquipType().getEqtypBasicLength(), false);
                            builder.append("final block pos " + finalblockPos)
                            //  ufv.setUfvLastKnownPosition(finalblockPos)
                        }

                    }
                }
            }
        }
        //  }
        return builder.toString()
    }

}