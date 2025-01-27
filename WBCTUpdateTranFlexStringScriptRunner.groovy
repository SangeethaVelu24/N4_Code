package WBCT

import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.road.RoadEntity
import com.navis.road.RoadField
import com.navis.road.business.model.TruckTransaction

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

class WBCTUpdateTranFlexStringScriptRunner {

    String execute() {
        StringBuilder sb = new StringBuilder()

        String tranNbr = "4523"
        sb.append("tran number " + tranNbr).append("\n")
        DomainQuery dq = QueryUtils.createDomainQuery(RoadEntity.TRUCK_TRANSACTION)
                .addDqPredicate(PredicateFactory.eq(RoadField.TRAN_NBR, tranNbr))

        TruckTransaction truckTran = (TruckTransaction) HibernateApi.getInstance().getUniqueEntityByDomainQuery(dq)
        sb.append("truck transaction " + truckTran).append("\n")
        if (truckTran != null) {
            truckTran.setTranFlexString04("6661")
        }
        sb.append("tran flex string04 " + truckTran.getTranFlexString04())
        return sb.toString()
    }
}
