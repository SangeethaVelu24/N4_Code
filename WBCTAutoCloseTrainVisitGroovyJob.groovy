package WBCT

import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.CarrierVisitPhaseEnum
import com.navis.argo.business.model.CarrierVisit
import com.navis.external.argo.AbstractGroovyJobCodeExtension
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.DateUtil
import com.navis.rail.RailEntity
import com.navis.rail.business.api.RailCompoundField
import com.navis.rail.business.entity.TrainVisitDetails
import org.apache.log4j.Logger

/*
* @Author: mailto: mailto:vsangeetha@weservetech.com, Sangeetha Velu; Date: 27-11-2024
*
*  Requirements: WBCT-266 - Auto close train visits in departed phase after 7 days
*
*
* @Inclusion Location: Incorporated as a code extension of the type
*
*  Load Code Extension to N4:
*  1. Go to Administration --> System --> Code Extensions
*  2. Click Add (+)
*  3. Enter the values as below:
*     Code Extension Name: WBCTAutoCloseTrainVisitGroovyJob
*     Code Extension Type: GROOVY_JOB_CODE_EXTENSION
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class WBCTAutoCloseTrainVisitGroovyJob extends AbstractGroovyJobCodeExtension {

    @Override
    void execute(Map<String, Object> inParams) {
        LOGGER.warn("WBCTAutoCloseTrainVisitGroovyJob executing...")
        List<TrainVisitDetails> trainVisitDetailsList = retrieveDepartedTrainVisit()

        boolean canCloseTrainVisit = false
        if (trainVisitDetailsList != null && trainVisitDetailsList.size() > 0) {
            for (TrainVisitDetails tvd : trainVisitDetailsList) {
                CarrierVisit cv = tvd.getCvdCv()
                if (cv != null) {
                    long atdDays = calculateDays(cv.getCvATD() != null ? cv.getCvATD() : null)
                    canCloseTrainVisit =  atdDays > ALLOWED_DAYS
                    if (canCloseTrainVisit) {
                        cv.setCvVisitPhase(CarrierVisitPhaseEnum.CLOSED)
                        HibernateApi.getInstance().save(cv)
                    }
                }
            }
        }
    }

    List<TrainVisitDetails> retrieveDepartedTrainVisit() {
        DomainQuery dq = QueryUtils.createDomainQuery(RailEntity.TRAIN_VISIT_DETAILS)
                .addDqPredicate(PredicateFactory.eq(RailCompoundField.CVD_CV_PHASE, CarrierVisitPhaseEnum.DEPARTED))

        return (List<TrainVisitDetails>) HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
    }

    long calculateDays(Date date) {
        if (date) {
            return DateUtil.differenceInCalendarDays(date, ArgoUtils.timeNow(), ContextHelper.getThreadUserTimezone())
        }
    }

    private static final long ALLOWED_DAYS = 7
    private static final Logger LOGGER = Logger.getLogger(WBCTAutoCloseTrainVisitGroovyJob.class)
}
