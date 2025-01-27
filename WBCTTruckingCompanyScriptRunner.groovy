package WBCT

import com.navis.argo.ArgoRefField
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.framework.business.Roastery
import com.navis.framework.business.atoms.LifeCycleStateEnum
import com.navis.framework.metafields.MetafieldId
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.Conjunction
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.Junction
import com.navis.framework.portal.query.PredicateFactory
import com.navis.road.RoadField
import com.navis.road.business.model.TruckingCompanyLine

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

class WBCTTruckingCompanyScriptRunner {

    String execute() {
        StringBuilder sb = new StringBuilder()
        String parmTruckCo = ""
        String parmStreamShipCo = "ANL"
        String parmLifeCycleState = "OK"
        String parmSSCOTruckerCode = ""
        DomainQuery dq = QueryUtils.createDomainQuery("TruckingCompany")
        if (parmTruckCo != "") {
            Junction tcCodeNamePredicate = PredicateFactory.disjunction()
                    .add(PredicateFactory.like(ArgoRefField.BZU_ID, parmTruckCo + "%"))
                    .add(PredicateFactory.like(ArgoRefField.BZU_NAME, parmTruckCo + "%"));

            dq.addDqPredicate(tcCodeNamePredicate)
        }

        if (parmStreamShipCo != "") {
            ScopedBizUnit scopedBizUnit = ScopedBizUnit.findEquipmentOperator(parmStreamShipCo)
            //  DomainQuery query = QueryUtils.createDomainQuery("TruckingCompanyLine")
           /* dq.addDqPredicate(PredicateFactory.eq(RoadField.TRKCLINE_LINE, scopedBizUnit.getBzuGkey()))
            sb.append("scoped biz unit " + scopedBizUnit).append("\n")
            sb.append("query " + dq).append("\n")

            Conjunction trkcLinePredicate = PredicateFactory.conjunction()
                    .add(PredicateFactory.eq(MetafieldIdFactory.valueOf("trkclineTrkco.trkcStatus"), parmLifeCycleState))
            dq.addDqPredicate(trkcLinePredicate)
*/

                    dq.addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("trkcStatus"), parmLifeCycleState))



            Conjunction trkcLinePredicate = PredicateFactory.conjunction()
            dq.addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("trkcLines.trkclineLine.bzuGkey"), scopedBizUnit.getBzuGkey()))
            dq.addDqPredicate(trkcLinePredicate)

            //dq.addDqPredicate(trkcLinePredicate)


            sb.append("dq " + dq).append("\n")
            List listTruckingCos = Roastery.getHibernateApi().findEntitiesByDomainQuery(dq)
            sb.append("list trucking co " + listTruckingCos).append("\n")
            /*Conjunction trkcLinePredicate = PredicateFactory.conjunction()
                    .add(PredicateFactory.eq(MetafieldIdFactory.valueOf("trkclineLine.bzuId"), parmStreamShipCo))
                    .add(PredicateFactory.eq(RoadField.TRKC_STATUS, parmLifeCycleState))

            dq.addDqPredicate(trkcLinePredicate)
            sb.append("trck line " + trkcLinePredicate).append("\n")*/
        }
//Get only Active Life Cycle State
        // dq.addDqPredicate(PredicateFactory.eq(ArgoRefField.BZU_LIFE_CYCLE_STATE, LifeCycleStateEnum.ACTIVE))

        //Order by Code
        // dq.addDqOrdering(Ordering.asc(ArgoRefField.BZU_ID));

        //  sb.append("dq " + dq).append("\n")
        // sb.append("list trucking co " + listTruckingCos)
        return sb.toString()
    }

    private static final MetafieldId trkc_line = MetafieldIdFactory.getCompoundMetafieldId(RoadField.TRKC_LINES, RoadField.TRKCLINE_LINE)

}
