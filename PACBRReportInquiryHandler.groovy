package WBCT

import com.navis.argo.business.api.ArgoUtils
import com.navis.argo.util.XmlUtil
import com.navis.external.argo.AbstractArgoCustomWSHandler
import com.navis.framework.portal.UserContext
import com.navis.framework.util.message.MessageCollector
import org.apache.log4j.Logger
import org.jdom.Element
import org.jdom.Namespace

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
*     Code Extension Name: PACBRReportInquiryHandler
*     Code Extension Type: WS_ARGO_CUSTOM_HANDLER
*  4. Click Save button
*
*  @Setup:
*
*  S.No    Modified Date   Modified By     Jira         Description
*
*/

class PACBRReportInquiryHandler extends AbstractArgoCustomWSHandler {

    @Override
    void execute(UserContext userContext, MessageCollector messageCollector, Element inElement, Element outElement, Long aLong) {
        Element rootElement = inElement.getChild(CUSTOM_CBR_REPORT)
        Element cbrElement = rootElement.getChild(CBR)
        String cbrNbr = cbrElement.getAttributeValue(CBR_NBR)
        String status = cbrElement.getAttributeValue(STATUS)
        String submittedFrom = cbrElement.getAttributeValue(SUBMITTED_FROM)
        String submittedTo = cbrElement.getAttributeValue(SUBMITTED_TO)
        String lineOp = cbrElement.getAttributeValue(LINE_OP)

        if (ArgoUtils.isNotEmpty(lineOp)) {
            Element responseCbrs = new Element(CBRS)
            Element responseCbr = new Element(CBR)

            XmlUtil.setOptionalAttribute(responseCbr, CBR_NBR, "", (Namespace) null);
            XmlUtil.setOptionalAttribute(responseCbr, OPERATION_TYPE, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, SOURCE_BOOKING_NBR, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, TARGET_BOOKING_NBR, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, SOURCE_POD, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, TARGET_POD, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, SOURCE_VESSEL_VISIT, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, TARGET_VESSEL_VISIT, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, SOURCE_VESSEL_NAME, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, TARGET_VESSEL_NAME, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, STATUS, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, TIME_SUBMITTED, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, TIME_REVIEWED, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, SSCO, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, ORIGINATOR, "", (Namespace) null)
            XmlUtil.setOptionalAttribute(responseCbr, REMARKS, "", (Namespace) null)

            responseCbrs.addContent(responseCbr)
            outElement.addContent(responseCbrs)
        }
    }


    private static final String CUSTOM_CBR_REPORT = "custom-cbr-report"
    private static final String SUBMITTED_FROM = "submitted-from"
    private static final String SUBMITTED_TO = "submitted-to"
    private static final String LINE_OP = "line-op"
    private static final String CBRS = "cbrs"
    private static final String CBR = "cbr"
    private static final String CBR_NBR = "cbr-nbr"
    private static final String OPERATION_TYPE = "operation-type"
    private static final String SOURCE_BOOKING_NBR = "source-booking-nbr"
    private static final String TARGET_BOOKING_NBR = "target-booking-nbr"
    private static final String SOURCE_POD = "source-pod"
    private static final String TARGET_POD = "target-pod"
    private static final String SOURCE_VESSEL_VISIT = "source-vessel-visit"
    private static final String TARGET_VESSEL_VISIT = "target-vessel-visit"
    private static final String SOURCE_VESSEL_NAME = "source-vessel-name"
    private static final String TARGET_VESSEL_NAME = "target-vessel-name"
    private static final String STATUS = "status"
    private static final String TIME_SUBMITTED = "time-submitted"
    private static final String TIME_REVIEWED = "time-reviewed"
    private static final String SSCO = "ssco"
    private static final String ORIGINATOR = "originator"
    private static final String REMARKS = "remarks"
    private static final Logger LOGGER = Logger.getLogger(PACBRReportInquiryHandler.class);

}
