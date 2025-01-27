import com.navis.extension.portal.ExtensionBeanUtils
import com.navis.extension.portal.IExtensionTransactionHandler
import com.navis.external.framework.beans.EBean
import com.navis.framework.extension.FrameworkExtensionTypes
import com.navis.framework.portal.FieldChanges
import com.navis.framework.portal.UserContext
import com.navis.framework.presentation.context.PresentationContextUtils
import com.navis.framework.presentation.context.RequestContext
import com.navis.framework.presentation.ui.CarinaButton
import com.navis.framework.presentation.ui.IFormButtonView
import com.navis.framework.presentation.util.FrameworkUserActions
import com.navis.framework.presentation.util.PresentationConstants
import com.navis.framework.presentation.util.UserAction
import com.navis.framework.util.message.MessageCollector
import com.navis.road.presentation.controller.CancelTranFormController
import org.apache.log4j.Logger

/*
*Author <a href="mailto:gtharani@weservetech.com">Tharani G</a>,10/09/2024
*
* Requirement :  [WBCT-81] --> Gate-10 Notify GOS of Cancellations
*
* @Inclusion Location: Incorporated as a code extension of the type BEAN_PROTOTYPE
*
* Load Code Extension to N4:
         *  1. Go to Administration --> System --> Code Extensions
         *  2. Click Add (+)
         *  3. Enter the values as below:
             Code Extension Name: customBeanPATranCancelFormController
             Code Extension Type: BEAN_PROTOTYPE
             Groovy Code: Copy and paste the contents of groovy code.
         *  4. Click Save button
* S.No      Modified Date      Modified By          Jira                      Description
* 1.        16-Dec-24          Sangeetha Velu       WBCT -81            calls the RectifySuggestedTransaction() method in afterValuesAssigned() method.
*/

class customBeanPATranCancelFormController extends CancelTranFormController implements EBean {

    private static final String MAP_KEY = "gkeys";
    private static final String TRANSACTION_BUSINESS = "PAGateTranCancelFormTransactionBusiness";

    @Override
    protected void afterValuesAssigned() {
        /* logger.warn("after value assigned executing...")
         FieldChanges fc = getFieldChanges(false)

         RectifySuggestedTransaction(fc)
         logger.warn("fc " + fc)*/
        super.afterValuesAssigned()
    }

      @Override
      protected void submit() {
          logger.warn("super submit ex...")
          super.submit();
          logger.warn("after value assigned ex...")
          FieldChanges fc = getFieldChanges(false)
          CarinaButton executeButton = getButton(FrameworkUserActions.EXECUTE);
          IFormButtonView buttonView = getFormButtonView()
          logger.warn("button view " + buttonView)
          UserAction userAction = buttonView.getUserActionForButton(executeButton);
          logger.warn("user action " + userAction)

          if (userAction != null) {
              logger.warn("user action is not null " + userAction.toString())
              RectifySuggestedTransaction(fc)
              logger.warn("fc " + fc)
          }
      }

    void RectifySuggestedTransaction(def fc) {
        logger.warn("RectifySuggestedContainer executing...")
        List<Serializable> tranGkeys = (List<Serializable>) getAttribute(PresentationConstants.SOURCE);
        final IExtensionTransactionHandler handler = ExtensionBeanUtils.getExtensionTransactionHandler();
        RequestContext requestContext = PresentationContextUtils.getRequestContext();
        UserContext context = requestContext.getUserContext();
        Map parms = new HashMap();
        parms.put(MAP_KEY, tranGkeys);
        parms.put("FIELD_CHANGES", fc)
        logger.warn("parms " + parms)
        Map results = new HashMap();

        MessageCollector mc = handler.executeInTransaction(context,
                FrameworkExtensionTypes.TRANSACTED_BUSINESS_FUNCTION, TRANSACTION_BUSINESS, parms, results);
    }
    private static final Logger logger = Logger.getLogger(customBeanPATranCancelFormController.class)

}

