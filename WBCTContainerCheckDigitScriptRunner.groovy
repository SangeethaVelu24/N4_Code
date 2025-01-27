package WBCT

import com.navis.argo.ArgoRefField
import com.navis.argo.business.atoms.CheckDigitAlgorithmEnum
import com.navis.argo.business.reference.Container
import com.navis.argo.business.reference.EquipPrefix
import com.navis.argo.business.reference.Equipment
import com.navis.framework.persistence.HibernateApi
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

class WBCTContainerCheckDigitScriptRunner {

    String execute() {
        StringBuilder sb = new StringBuilder()
        String lookupId = "FFAU53908770"
        lookupId = lookupId.substring(0, 10)
        DomainQuery dq = QueryUtils.createDomainQuery("Container")
                .addDqPredicate(PredicateFactory.eq(ArgoRefField.EQ_ID_NO_CHECK_DIGIT, lookupId));
        sb.append("dq "+dq).append("\n")
        Container ctr = (Container) HibernateApi.getInstance().getUniqueEntityByDomainQuery(dq);
        sb.append("ctr "+ctr).append("\n")
        sb.append("equip "+ Equipment.loadEquipment(lookupId))
        return sb.toString()
    }

    public static boolean isCtrValid(String inEqId) {
        boolean isCtr = false;
        if (inEqId.length() == 11 && inEqId.charAt(3) == 'U') {
            CheckDigitAlgorithmEnum checkDigitAlgorithm = EquipPrefix.findEquipPrefixCheckDigitAlgorithm(inEqId);
            if (!CheckDigitAlgorithmEnum.PARTOFID.equals(checkDigitAlgorithm)) {
                isCtr = true;
            }
        }
        return isCtr;
    }
}
