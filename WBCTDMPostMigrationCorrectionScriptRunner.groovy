package WBCT

import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.cargo.InventoryCargoEntity
import com.navis.cargo.InventoryCargoField
import com.navis.cargo.business.model.BillOfLading
import com.navis.cargo.business.model.BlGoodsBl
import com.navis.external.argo.AbstractGroovyJobCodeExtension
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import org.apache.log4j.Level
import org.apache.log4j.Logger

/*
 * Description: Groovy to correct bls
 *
 */

public class WBCTDMPostMigrationCorrectionScriptRunner {



    String execute() {
        LOGGER.setLevel(Level.DEBUG)
        LOGGER.debug("WBCTDMPostMigrationCorrections begin !!!!")
        correctBillsOfLading()
        // correctContainerTypes()
        //  correctGensetTypes()

        return "success"
    }

    void correctBillsOfLading() {
        DomainQuery query = QueryUtils.createDomainQuery(InventoryCargoEntity.BILL_OF_LADING)
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("blCreator"), "MIGRATION"))
        // .addDqPredicate(PredicateFactory.isNotNull(MetafieldIdFactory.valueOf("blLineOperator")))

          .addDqPredicate(PredicateFactory.like(InventoryCargoField.BL_NBR, "YMLU%"))
        //.setDqMaxResults(100)
        List bls = HibernateApi.instance.findEntitiesByDomainQuery(query)
        for (int i = 0; i < bls.size(); i++) {
            BillOfLading bl = (BillOfLading) bls.get(i)
            String oldNumber = bl.blNbr
            if (bl.getBlLineOperator() != null && bl.getBlNbr() != null) {
                String newNumber = fixBLNumber(bl.getBlNbr(), bl.getBlLineOperator())

                if (newNumber != oldNumber) {
                    if (BillOfLading.findBillOfLading(newNumber, bl.blLineOperator, bl.blCarrierVisit) == null) {
                        bl.blNbr = newNumber
                        HibernateApi.instance.save(bl)
                        // log("BL number ${oldNumber} fixed --> ${newNumber}")
                    } else {

                    }
                    //  log("BL ${newNumber} already exists.  ${oldNumber} not fixed.")
                }

                if ((bl.blBlGoodsBls != null) && (bl.blBlGoodsBls.size() > 0)) {
                    for (int j = 0; j < bl.blBlGoodsBls.size(); j++) {
                        BlGoodsBl blGoodsBl = (BlGoodsBl) bl.blBlGoodsBls.getAt(j)
                        if (blGoodsBl.blgdsblGoodsBl.gdsBlNbr != newNumber) {
                            blGoodsBl.blgdsblGoodsBl.gdsBlNbr = newNumber
                            HibernateApi.instance.save(blGoodsBl.blgdsblGoodsBl)
                        }
                    }
                }
            }
        }
    }

    /*private void correctContainerTypes() {
        DomainQuery query = QueryUtils.createDomainQuery(EquipType.simpleName)
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("eqtypClass"), EquipClassEnum.CONTAINER))

        List eqTypes = HibernateApi.instance.findEntitiesByDomainQuery(query)

        for (int i = 0; i < eqTypes.size(); i++) {
            EquipType eqType = (EquipType) eqTypes.get(i)
            if (containerHeightsMap.containsKey(eqType.eqtypId) &&
                    (eqType.eqtypNominalHeight != containerHeightsMap.get(eqType.eqtypId))) {
                eqType.eqtypNominalHeight = containerHeightsMap.get(eqType.eqtypId)
                HibernateApi.instance.save(eqType)
                //  log("Height corrected for ${eqType.eqtypId}")
            }
        }
    }*/

    private String fixBLNumber(String blNo, ScopedBizUnit lineOp) {
        // LOGGER.debug("fixBLNumber::: "+ blNo)
        if ((blNo == null) || (blNo == "") || (blNo == 'DELETED')) {
            blNo = null
        } else if (blNo != null && lineOp != null && lineOp.getBzuScac() != null && (blNo.startsWith(lineOp.getBzuScac()) ||blNo.startsWith("MEDU") || blNo.startsWith("YMLU"))) {
            blNo = blNo.substring(lineOp.bzuScac.size())
        }
        return blNo
    }


    /* private void correctGensetTypes() {
         DomainQuery query = QueryUtils.createDomainQuery(EquipType.simpleName)
                 .addDqPredicate(PredicateFactory.in(MetafieldIdFactory.valueOf("eqtypId"), ['NMGS', 'USGS'].toArray()))
         List eqTypes = HibernateApi.instance.findEntitiesByDomainQuery(query)

         for (int i = 0; i < eqTypes.size(); i++) {
             EquipType eqType = (EquipType) eqTypes.get(i)
             eqType.eqtypNominalLength = EquipNominalLengthEnum.NOM40
             eqType.eqtypNominalHeight = EquipNominalHeightEnum.NOM86
             eqType.eqtypIsoGroup = (eqType.eqtypId == 'NMGS' ? EquipIsoGroupEnum.GS : EquipIsoGroupEnum.GU)
             eqType.eqtypIsArchetype = true
             eqType.eqtypArchetype = eqType
             eqType.eqtypTareWeightKg = ((Double) 5000 * 0.4535923699997481).round()      // convert 5000LBs to KG
             eqType.eqtypSafeWeightKg = 0
             eqType.eqtypClass = EquipClassEnum.ACCESSORY
             eqType.eqtypRfrType = EquipRfrTypeEnum.NON_RFR
             eqType.eqtypLengthMm = 12192
             eqType.eqtypHeightMm = 2591
             eqType.eqtypWidthMm = 0
             HibernateApi.instance.save(eqType)
             // log("Corrected genset type ${eqType.eqtypId}")
         }
     }*/

    /*private Map<String, EquipNominalHeightEnum> containerHeightsMap =
            [
                    '24G0': EquipNominalHeightEnum.NOM96,
                    '24U0': EquipNominalHeightEnum.NOM96,
                    '40P1': EquipNominalHeightEnum.NOM96,
                    '44G0': EquipNominalHeightEnum.NOM96,
                    '44P0': EquipNominalHeightEnum.NOM96,
                    '44P1': EquipNominalHeightEnum.NOM96,
                    '44P3': EquipNominalHeightEnum.NOM96,
                    '44R0': EquipNominalHeightEnum.NOM96,
                    '44U0': EquipNominalHeightEnum.NOM96,
                    'L4G0': EquipNominalHeightEnum.NOM96,
                    'L4R0': EquipNominalHeightEnum.NOM96,
                    'M4G0': EquipNominalHeightEnum.NOM96
            ]*/
    private static final Logger LOGGER = Logger.getLogger(this.class)
}
