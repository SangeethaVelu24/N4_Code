package WBCT

import com.navis.argo.business.reference.Group
import com.navis.framework.business.atoms.LifeCycleStateEnum
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory

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

class WBCTUpdateGroupScriptRunner {

    String execute() {
        StringBuilder sb = new StringBuilder()

        DomainQuery dq = QueryUtils.createDomainQuery("Group")
                .addDqPredicate(PredicateFactory.isNotNull(MetafieldIdFactory.valueOf("grpId")))
                .addDqPredicate(PredicateFactory.eq(MetafieldIdFactory.valueOf("grpLifeCycleState"), LifeCycleStateEnum.ACTIVE))
                .addDqOrdering(Ordering.asc(MetafieldIdFactory.valueOf("grpId")))

        List<Group> groupList = (List<Group>) HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
        List<String> grpIdList = new ArrayList<>()
        sb.append("group list " + groupList.size()).append("\n")
        if (groupList != null && groupList.size() > 0) {
            for (Group group : groupList) {
                String groupId = group.getGrpId() != null ? group.getGrpId() : null

                  if (groupId != null && groupId.endsWith(".")) {
                      groupId = groupId.replace(".", "")
                      group.setGrpId(groupId)
                      group.setGrpDescription(groupId)
                      grpIdList.add(groupId)
                  }
              /*  if ("4I".equals(groupId)) {
                    sb.append("4I").append("\n")
                    group.purge()
                    sb.append("purge completed")
                }*/
            }
            sb.append("grpId list " + grpIdList).append("\n")
        }
        return sb.toString()
    }
}
