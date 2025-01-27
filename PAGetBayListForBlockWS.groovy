package WBCT

import com.navis.argo.ContextHelper
import com.navis.argo.business.model.Yard
import com.navis.external.argo.AbstractGroovyWSCodeExtension
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.spatial.BinField
import com.navis.spatial.BlockField
import com.navis.yard.business.atoms.YardBlockTypeEnum
import com.navis.yard.business.model.AbstractYardBlock
import com.navis.yard.business.model.YardBinModel
import com.navis.yard.business.model.YardSection
import groovy.json.JsonOutput
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 06-09-2024
*
*  Requirements: retrieves the bay list from the block id
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: PAGetBayListForBlockWS
*     Code Extension Type: GROOVY_WS_CODE_EXTENSION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class PAGetBayListForBlockWS extends AbstractGroovyWSCodeExtension {

    @Override
    String execute(Map<String, String> inParams) {
        LOGGER.warn("PAGetBayListForBlockWS executing...")
        String blockId = inParams.get("BLOCK_ID")
        List<Map<String, Object>> blockList = new ArrayList<Map<String, Object>>();

        if (blockId == null) {
            Map<String, Object> error = new HashMap<String, Object>();
            error["ERROR"] = "No containers passed";
            blockList.add(error);
            return JsonOutput.toJson(blockList);
        };
        if (blockId != null) {
            String bay = null
            Yard thisYard = ContextHelper.getThreadYard();
            YardBinModel yardModel = (YardBinModel) HibernateApi.getInstance().downcast(thisYard?.getYrdBinModel(), YardBinModel.class)
            AbstractYardBlock yardBlock = AbstractYardBlock.findAbstractYardBlock(yardModel, blockId)
            if (yardBlock != null) {
                if (YardBlockTypeEnum.TRANSTAINER.equals(yardBlock.getAyblkBlockType())
                        || YardBlockTypeEnum.FORKLIFT.equals(yardBlock.getAyblkBlockType())) {
                    DomainQuery query = QueryUtils.createDomainQuery("YardSection").addDqPredicate(PredicateFactory.eq(BinField.ABN_PARENT_BIN, yardBlock.getAbnGkey()));
                    query.addDqOrdering(Ordering.asc(BlockField.ASN_ROW_INDEX));
                    List<YardSection> yardSectionList = (List<YardSection>) HibernateApi.getInstance().findEntitiesByDomainQuery(query)
                    if (yardSectionList != null && yardSectionList.size() > 0) {
                        for (YardSection yardSection : yardSectionList) {
                            if (yardSection.getAbnName() != null) {
                                bay = yardSection.getAbnName().substring(yardSection.getAbnName().length() - 2, yardSection.getAbnName().length())
                                if (bay != null) {
                                    Map<String, Object> blockMap = new HashMap<String, Object>();
                                    blockMap["Code"] = bay
                                    blockMap["Name"] = bay
                                    blockList.add(blockMap)
                                }
                            }
                        }
                    }
                }
            }
        }
        return JsonOutput.toJson(blockList);
    }
    private static final Logger LOGGER = Logger.getLogger(PAGetBayListForBlockWS.class);
}
