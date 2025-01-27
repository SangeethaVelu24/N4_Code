import com.navis.argo.ContextHelper
import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.business.atoms.UnitCategoryEnum
import com.navis.argo.business.model.GeneralReference
import com.navis.argo.business.reference.HardwareDevice
import com.navis.argo.business.reports.ReportDesign
import com.navis.argo.util.PrintUtil
import com.navis.framework.business.Roastery
import com.navis.framework.email.DefaultAttachment
import com.navis.framework.email.EmailManager
import com.navis.framework.email.EmailMessage
import com.navis.framework.persistence.HibernateApi
import com.navis.inventory.business.units.Unit
import com.navis.road.business.reference.Printer
import com.navis.services.business.event.Event
import com.navis.services.business.event.GroovyEvent
import net.sf.jasperreports.engine.*
import net.sf.jasperreports.engine.data.JRMapCollectionDataSource
import net.sf.jasperreports.engine.export.JRPdfExporter
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.hibernate.SQLQuery
import org.hibernate.transform.AliasToEntityMapResultTransformer
import org.jetbrains.annotations.NotNull
import org.jetbrains.annotations.Nullable
import org.springframework.core.io.ByteArrayResource

import java.text.SimpleDateFormat

/*
*
* @Author <a href="mailto:rkarthikeyan@weservetech.com">Karthiekayn R</a>, 20/July/2020
*
* Requirements : This groovy is used to generate reefer report and send via mail/printer
*
* Include new report ( UNIT_REQUEST_UNPLUG)  : 2022-02-24
*
*/


class GvyGenerateReeferReportServiceEvent {

    private Logger LOGGER = Logger.getLogger(GvyGenerateReeferReportServiceEvent.class)
    private static final String emailFrom = "mharikumar@weservetech.com"

    public void execute(final GroovyEvent groovyEvent) {
        LOGGER.setLevel(Level.DEBUG)
        logMsg("GvyGenerateReeferReportServiceEvent - Begin")

        GeneralReference genRef = GeneralReference.findUniqueEntryById("REPORT", "REEFER_LOG", "IMPORT_EXPORT")
        if (genRef != null) {
            try {
                Date date = new Date()
                SimpleDateFormat formatter = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss")
                SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

                Event event = groovyEvent.getEvent()
                logMsg("Event is " + event.getEvntEventType().getEvnttypeId())

                String evntAppliedDate = dateFormatter.format(event.getEvntAppliedDate())
                logMsg("Event applied Date  " + evntAppliedDate)

                Unit unit = (Unit) groovyEvent.getEntity()
                String unitID = unit.getUnitId()
                logMsg("Unit unitID " + unitID)
                logMsg("category  :" + unit.getUnitCategory())

                String eventType = event.getEvntEventType().getEvnttypeId()
                String printerID = null
                String reportDesignName = null
                String suffix = " - "

                if ((UnitCategoryEnum.IMPORT.equals(unit.getUnitCategory()))) {
                    printerID = genRef.getRefValue4()
                    if ("UNIT_POSITION_CORRECTION".equals(eventType)) {
                        reportDesignName = "N4_REEFER_IMPORT_REEFER_HAS_BEEN_SPOTTED"
                        suffix = " Reefer Import Has Been Spotted"
                    } else if ("UNIT_DISCH".equals(eventType)) {
                        reportDesignName = "N4_REEFER_INSPECTION_FORM_BY_NBR_1A_EDITED_IMPORTS"
                        suffix = " Reefer Inspection Edited Import"
                    } else if ("UNIT_REQUEST_UNPLUG".equals(eventType)) {
                        reportDesignName = "N4_REEFER_IMPORT_REEFER_UNPLUG_NOTICE_V1"
                        suffix = " Reefer Unit Request Unplug"
                    }
                } else if ((UnitCategoryEnum.EXPORT.equals(unit.getUnitCategory()))) {
                    printerID = genRef.getRefValue5()
                    if ("UNIT_POSITION_CORRECTION".equals(eventType)) {
                        reportDesignName = "N4_REEFER_EXPORT_REEFER_HAS_BEEN_SPOTTED"
                        suffix = " Reefer Export Has Been Spotted"
                    } else if ("UNIT_RECEIVE".equals(eventType)) {
                        reportDesignName = "N4_REEFER_INSPECTION_EDITED_GATE_EXPORTS"
                        suffix = " Reefer Inspection Edited Gate Export"
                    } else if ("UNIT_DERAMP".equals(eventType)) {
                        reportDesignName = "N4_REEFER_INSPECTION_EDITED_RAIL_EXPORTS"
                        suffix = " Reefer Inspection Edited Rail Export"
                    }
                }

                String subject = genRef.getRefValue1() + suffix
                String body = genRef.getRefValue2() + suffix
                String emailTo = genRef.getRefValue3()
                logMsg("genRef  :" + subject + " : " + body + " : " + emailTo + " : " + printerID)
                logMsg("reportDesignName  :" + reportDesignName)

                if (reportDesignName != null & (emailTo != null || printerID != null)) {
                    processReport(unitID, reportDesignName, emailTo, subject, body + " - " + formatter.format(date), printerID, evntAppliedDate)
                }
            } catch (Exception e) {
                e.printStackTrace()
                logMsg("Exception  generateReport : " + e)
            }
        } else {
            logMsg("GeneralReference  not found ")
        }
    }

    public void processReport(String unitID, String reportDefinitionName, String emailTo, String subject, String body, String printerID, String evntAppliedDate) {
        try {
            logMsg("processReport ...........")
            HashMap parameters = new HashMap()
            ByteArrayResource bar = new ByteArrayResource(generateReportDesign(unitID, parameters, reportDefinitionName, printerID, evntAppliedDate))
            logMsg("bar " + bar)
            composeEmail(reportDefinitionName, emailTo, subject, body, bar)
            logMsg("email send successfully")
        } catch (Exception e) {
            e.printStackTrace()
            logMsg("Exception  emailReport : " + e)
        }
    }

    public byte[] generateReportDesign(String unitID, Map parameters, String reportDefinitionName, String printerID, String evntAppliedDate) {
        logMsg("generateReportDesign......... ")
        String xml = null
        xml = ReportDesign.findReportDesign(reportDefinitionName, com.navis.argo.business.atoms.ScopeEnum.YARD).getRepdesXmlContent()
        logMsg("xml " + xml)
        InputStream inputStream = new ByteArrayInputStream(xml.getBytes())
        return createJaperReport(unitID, inputStream, parameters, printerID, evntAppliedDate)
    }

    public byte[] createJaperReport(String unitID, InputStream inputStream, Map parameters, String printerID, String evntAppliedDate) {

        logMsg("createJaperReport.........  ")
        ByteArrayOutputStream pdfByteArray = new ByteArrayOutputStream()
        try {
            StringBuilder sbf = new StringBuilder();
            JasperReport report = JasperCompileManager.compileReport(inputStream)
            JRQuery query = report.getQuery()
            sbf.append(query.getText())
            sbf.append("\n  and unit.id = '" + unitID + "'")
            sbf.append("\n  and se.placed_time >= TO_DATE('" + evntAppliedDate + "','YYYY-MM-DD HH24:MI:SS')")

            logMsg(" Query " + sbf.toString())
            List<Map<String, Object>> maplist = getQueryResultMap(sbf.toString())
            logMsg("map list " + maplist)
            if (!maplist.isEmpty() && maplist != null) {
                logMsg(" maplist size  " + maplist.size())
            }

            JRDataSource ds = new JRMapCollectionDataSource(maplist)
            JasperPrint print = JasperFillManager.fillReport(report, parameters, ds)
            JRPdfExporter exporterPDF = new JRPdfExporter()
            exporterPDF.setParameter(JRExporterParameter.JASPER_PRINT, print)
            exporterPDF.setParameter(JRExporterParameter.OUTPUT_STREAM, pdfByteArray)
            exporterPDF.exportReport()
            logMsg("ds " + ds)
            logMsg("query " + query)
            logMsg("report " + report)
            logMsg("print " + print)
            logMsg("pdf byte stream " + pdfByteArray)
            logMsg("exporterPDF " + exporterPDF)
            /*  if (printerID != null && maplist != null && !maplist.isEmpty()) {
                  Printer printer = HardwareDevice.findHardwareDeviceById(printerID) as Printer
                  logMsg(" Printer address : " + printer.getHwHostAddress())
                  PrintUtil.print(pdfByteArray.toByteArray(), printer.getHwHostAddress(), printer.getPrtrQueueName(), 1)
                  logMsg("Print completed............. ")
              }*/

        } catch (Exception e) {
            e.printStackTrace()
            logMsg("Exception createJaperReport :  " + e.getMessage())
        }
        return pdfByteArray.toByteArray()
    }

    public void composeEmail(String reportDefinitionName, String emailTo, String subject, String body, ByteArrayResource barAttachment) {
        logMsg(" composeEmail......... ")
       // try {
            logMsg("email to " + emailTo)
            if (emailTo != null) {
                EmailMessage msg = new EmailMessage(ContextHelper.getThreadUserContext())
                msg.setTo(StringUtils.split(emailTo, ","))
                //msg.setSubject(getEnvVersion()+subject)
                msg.setSentDate(ArgoUtils.timeNow())
                msg.setSubject(subject)
                msg.setText(body)
                msg.setReplyTo(emailTo)
                msg.setFrom(emailFrom)

                DefaultAttachment attach = new DefaultAttachment()
                attach.setAttachmentContents(barAttachment)
                attach.setAttachmentName(reportDefinitionName + ".pdf")
                attach.setContentType("application/octet-stream")
                msg.addAttachment(attach)
                logMsg("email message "+msg)
                def emailManager = Roastery.getBean("emailManager")
                EmailManager mng = new EmailManager()
                mng.sendEmail(msg)
            }
       /* } catch (Exception e) {
            e.printStackTrace()
            logMsg("Exception  composeEmail : " + e)
        }*/
    }

    @Nullable
    public List<Map<String, Object>> getQueryResultMap(@NotNull String inSqlString) {
        try {
            SQLQuery query = HibernateApi.getInstance().getCurrentSession().createSQLQuery(inSqlString)
            query.setResultTransformer(AliasToEntityMapResultTransformer.INSTANCE)
            List<Map<String, Object>> aliasToValueMapList = query.list()
            return aliasToValueMapList
        } catch (Exception e) {
            logMsg("Exception  getQueryResultMap : " + e)
        }
    }

    private void logMsg(String inMsg) {
        LOGGER.debug(inMsg)
    }
}
