import com.navis.argo.ArgoIntegrationEntity
import com.navis.argo.ArgoIntegrationField
import com.navis.argo.ArgoRefEntity
import com.navis.argo.ArgoRefField
import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.BizRoleEnum
import com.navis.argo.business.atoms.CarrierVisitPhaseEnum
import com.navis.argo.business.atoms.LogicalEntityEnum
import com.navis.argo.business.integration.IntegrationServiceMessage
import com.navis.argo.business.model.CarrierVisit
import com.navis.argo.business.reference.Group
import com.navis.argo.business.reference.ScopedBizUnit
import com.navis.framework.business.atoms.LifeCycleStateEnum
import com.navis.framework.metafields.MetafieldIdFactory
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.Disjunction
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.Junction
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.DateUtil
import com.navis.rail.RailEntity
import com.navis.rail.business.api.RailCompoundField
import com.navis.rail.business.entity.TrainVisitDetails
import com.navis.road.RoadEntity
import com.navis.road.RoadField
import com.navis.road.business.model.TruckingCompany
import org.apache.commons.lang.StringUtils

import java.text.SimpleDateFormat
import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class WBCTScriptRunner {

    String execute() {
        StringBuilder sb = new StringBuilder()
        /*   String apptError = "APPT_LATE|0400-0500|2024-10-21T00:44"
           String slot = ""
           String delay = "0mins"
           if (apptError != null && StringUtils.isNotEmpty(apptError.trim())) {
               if (apptError.contains('|')) {
                   sb.append("appt error string " + apptError).append("\n")
                   String[] apptSlotError = apptError.split('\\|')
                   sb.append("appt slot error " + apptSlotError).append("\n")
                   apptError = apptSlotError[0]
                   slot = apptSlotError[1] != null ? apptSlotError[1].replace('_', "-") : ""
                   sb.append("slot " + slot).append("\n")
                   if (apptError.contains("LATE") && apptSlotError.length > 2) {
                       String apptDate = apptSlotError[2]
                       DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
                       LocalDateTime parsedDateTime = LocalDateTime.parse(apptDate, formatter);
                       LocalDateTime currentDateTime = LocalDateTime.now().withSecond(0).withNano(0);
                       sb.append("parsedDateTime " + parsedDateTime).append("\n")
                       sb.append("currentDateTime " + currentDateTime).append("\n")
                       if (parsedDateTime.isBefore(currentDateTime)) {
                           Duration duration = Duration.between(parsedDateTime, currentDateTime);
                           sb.append("duration " + duration).append("\n")

                       }
                   }
               }
           }*/
        //String performed = "20210609211954"
        /*   String performed = "2021-02-09 15:12:00.0"
         sb.append("performed " + performed).append("\n")

         Date pfDate = DateUtil.getTodaysDate(ContextHelper.getThreadUserContext().getTimeZone())
         sb.append("pf date today " + pfDate).append("\n")
         if (performed == null || performed == '' || performed == 'null')
             return null

         Date performedDate = performed ? new SimpleDateFormat('yyyy-MM-dd HH:mm:ss').parse(performed) : ''
         sb.append("performed date " + performedDate).append("\n")
         if (performed) {

             pfDate = DateUtil.dateStringToDate(performedDate.format('yyyy-MM-dd HH:mm:ss'))
             sb.append("pf date if performed " + pfDate).append("\n")
         }*/

        /* DomainQuery domainQuery = QueryUtils.createDomainQuery(ArgoIntegrationEntity.INTEGRATION_SERVICE_MESSAGE)
         domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_ENTITY_CLASS, LogicalEntityEnum.UNIT));
       //  domainQuery.addDqPredicate(PredicateFactory.eq(ArgoIntegrationField.ISM_USER_STRING3, "true"))
         domainQuery.addDqPredicate(PredicateFactory.isNotNull(ArgoIntegrationField.ISM_USER_STRING4))
         domainQuery.addDqOrdering(Ordering.asc(ArgoIntegrationField.ISM_SEQ_NBR))

         List<IntegrationServiceMessage> integrationServiceMessageList = HibernateApi.getInstance().findEntitiesByDomainQuery(domainQuery)

         if (integrationServiceMessageList != null && integrationServiceMessageList.size() > 0) {
             sb.append("size " + integrationServiceMessageList.size())
             for (IntegrationServiceMessage ism : integrationServiceMessageList) {
                 ism.setIsmUserString4(null)
             }
         }*/

        /*Junction activeJunction = PredicateFactory.conjunction()
                .add(PredicateFactory.gt(RailCompoundField.RVDTLS_A_T_D, getStartDate()))

        Junction completeJuction = PredicateFactory.conjunction()
                .add(PredicateFactory.gt(RailCompoundField.RVDTLS_TIME_END_WORK, getStartDate()))

        Disjunction disjunction = new Disjunction();
        disjunction.add(activeJunction);
        disjunction.add(completeJuction);

        DomainQuery dq = QueryUtils.createDomainQuery(RailEntity.TRAIN_VISIT_DETAILS)
                .addDqPredicate(PredicateFactory.eq(RailCompoundField.CVD_CV_PHASE, CarrierVisitPhaseEnum.DEPARTED))


        List<TrainVisitDetails> trainVisitDetailsList = (List<TrainVisitDetails>) HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
        int closeDays = 7
        boolean canCloseTrainVisit = false
        if (trainVisitDetailsList != null && trainVisitDetailsList.size() > 0) {
            for (TrainVisitDetails tvd : trainVisitDetailsList) {
                CarrierVisit cv = tvd.getCvdCv()
                sb.append("cv " + cv).append("\n")
                if (cv != null) {
                    sb.append("atd " + cv.cvATD).append("\n")
                    sb.append("tew " + tvd.getRvdtlsTimeEndWork()).append("\n")
                    long timeEndWorkDays = calculateDays(tvd?.getRvdtlsTimeEndWork() != null ? tvd.getRvdtlsTimeEndWork() : null)
                    long atdDays = calculateDays(cv.getCvATD() != null ? cv.getCvATD() : null)
                    canCloseTrainVisit = timeEndWorkDays > closeDays || atdDays > closeDays
                    sb.append("can close " + canCloseTrainVisit).append("\n")
                    sb.append("time end work " + timeEndWorkDays).append("\n")
                    sb.append("atd days " + atdDays).append("\n")
                    if (canCloseTrainVisit) {
                        cv.setCvVisitPhase(CarrierVisitPhaseEnum.CLOSED)
                    }
                }
            }
        }
        sb.append("date " + getStartDate()).append("\n")
        sb.append("dq " + dq).append("\n")
        sb.append("train visit details " + trainVisitDetailsList).append("\n")*/
        DomainQuery subQuery = QueryUtils.createDomainQuery(RoadEntity.TRUCKING_COMPANY)
                .addDqPredicate(PredicateFactory.eq(ArgoRefField.BZU_LIFE_CYCLE_STATE, LifeCycleStateEnum.ACTIVE))

        List<TruckingCompany> truckingCompanyList = HibernateApi.getInstance().findEntitiesByDomainQuery(subQuery)
        sb.append("trucking company list " + truckingCompanyList.size()).append("\n")
        Set<String> trkList = new HashSet<>()
        if (truckingCompanyList != null && truckingCompanyList.size() > 0) {
            for (TruckingCompany trkCompany : truckingCompanyList) {

                Set<Group> groupSet = trkCompany.getBzuGrpSet()
                ScopedBizUnit scopedBizUnit = ScopedBizUnit.findScopedBizUnit(trkCompany.getBzuId(), BizRoleEnum.HAULIER)

                // sb.append("scoped biz unit " + trkCompany?.getBzuId()).append("\n")
                // sb.append("scoped unit " + scopedBizUnit).append("\n")
                DomainQuery dq = QueryUtils.createDomainQuery(ArgoRefEntity.GROUP)
                        .addDqPredicate(PredicateFactory.eq(ArgoRefField.GRP_FLEX_STRING01, "Y"))
                        .addDqPredicate(PredicateFactory.eq(ArgoRefField.GRP_LIFE_CYCLE_STATE, LifeCycleStateEnum.ACTIVE))
                // .addDqPredicate(PredicateFactory.isNotNull(MetafieldIdFactory.valueOf("grpBzuSet.bzuId")))
                // sb.append("dq " + dq).append("\n")
                List<Group> groupList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
                // sb.append("group list " + groupList.size()).append("\n")
                for (Group group : groupSet) {

                    boolean isMember = group.isMember(scopedBizUnit)
                    if (isMember && "Y".equals(group.getGrpFlexString01()) && LifeCycleStateEnum.ACTIVE.equals(group.getGrpLifeCycleState())) {
                        trkList.add(trkCompany.getBzuId())
                    }
                }

            }
        }
     /*   DomainQuery dq = QueryUtils.createDomainQuery(ArgoRefEntity.GROUP)
                .addDqPredicate(PredicateFactory.eq(ArgoRefField.GRP_FLEX_STRING01, "Y"))
                .addDqPredicate(PredicateFactory.eq(ArgoRefField.GRP_LIFE_CYCLE_STATE, LifeCycleStateEnum.ACTIVE))

        List<Group> groupList = HibernateApi.getInstance().findEntitiesByDomainQuery(dq)
        Set<String> trkList = new HashSet<>()
        for (Group group : groupList) {

            Set<ScopedBizUnit> scopedBizUnitSet = group.getGrpBzuSet()
            if (scopedBizUnitSet != null && scopedBizUnitSet.size() > 0) {
                for (ScopedBizUnit scopedBizUnit : scopedBizUnitSet){
                    trkList.add(scopedBizUnit.getBzuId())
                }
            }
        }*/


            sb.append("trk list " + trkList.size()).append("\n")



        return sb.toString()
    }

    /* long calculateDays(Date date) {
         if (date) {
             return DateUtil.differenceInCalendarDays(date, ArgoUtils.timeNow(), ContextHelper.getThreadUserTimezone())
         }
     }

     public Date getStartDate() {
         Calendar cal = Calendar.getInstance();
         cal.add(Calendar.DAY_OF_WEEK, 7);
         return cal.getTime();
     }*/
}