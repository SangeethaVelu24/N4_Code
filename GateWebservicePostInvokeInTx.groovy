import com.navis.argo.ArgoField
import com.navis.argo.business.model.Complex
import com.navis.argo.business.model.Operator
import com.navis.argo.business.reference.Container
import com.navis.argo.business.reference.Equipment
import com.navis.argo.util.ArgoGroovyUtils
import com.navis.argo.util.XmlUtil
import com.navis.argo.webservice.types.v1_0.QueryResultType
import com.navis.framework.business.Roastery
import com.navis.framework.persistence.HibernateApi
import com.navis.framework.portal.Ordering
import com.navis.framework.portal.QueryUtils
import com.navis.framework.portal.query.DomainQuery
import com.navis.framework.portal.query.PredicateFactory
import com.navis.framework.util.internationalization.PropertyKey
import com.navis.framework.util.internationalization.PropertyKeyFactory
import com.navis.framework.util.message.MessageCollectorFactory
import com.navis.framework.util.message.MessageLevel
import com.navis.inventory.business.api.UnitFinder
import com.navis.inventory.business.imdg.HazardItem
import com.navis.inventory.business.imdg.Placard
import com.navis.inventory.business.units.EqBaseOrder
import com.navis.inventory.business.units.Unit
import com.navis.orders.business.eqorders.Booking
import com.navis.orders.business.eqorders.EquipmentOrder
import com.navis.road.RoadEntity
import com.navis.road.RoadField
import com.navis.road.business.atoms.TranStatusEnum
import com.navis.road.business.atoms.TranSubTypeEnum
import com.navis.road.business.model.DocumentMessage
import com.navis.road.business.model.TransactionPlacard
import com.navis.road.business.model.TruckTransaction
import com.navis.road.business.model.TruckVisitDetails
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.jdom.Document
import org.jdom.Element

/*
 *@Author <a href="mailto:rgopal@weservetech.com">Gopala Krishnan R</a>, 05/Nov/2024
 *
 * Requirements : WBCT - 332: This Groovy is used to add placards when transaction went to trouble using SMTP Request.
 *
 * @Inclusion Location	: Incorporated as GroovyPlugin
 *
 * Copy and paste it in Groovy Plugin.
 * Plug-In Name: GateWebservicePostInvokeInTx
 *
 */

class GateWebservicePostInvokeInTx {

    public void preHandlerInvoke(Map parameter) {

    }

    public void postHandlerInvoke(Map parameter) {
        log.warn("GateWebservicePostInvokeInTx postHandlerInvoke executing...")
        Element element = (Element) parameter.get(ArgoGroovyUtils.WS_ROOT_ELEMENT);
        Element submitTransactionsElement = element.getChild(SMTP) != null ?
                (Element) element.getChild(SMTP) : null;
        if (submitTransactionsElement != null) {
            String tvKey = submitTransactionsElement.getChild(TRUCK_VISIT)?.getAttributeValue(TV_KEY)
            TruckVisitDetails truckVisitDetails = TruckVisitDetails.findTVActiveByGkey(Long.parseLong(tvKey))
            if (truckVisitDetails != null) {
                Element transElement = submitTransactionsElement.getChild(TRUCK_TRANS)
                if (transElement != null) {
                    List trkTranList = transElement.getChildren(TRANSACTION)
                    if (trkTranList != null && !trkTranList.isEmpty()) {
                        for (Element truckTransElement : trkTranList) {
                            Element containerElement = truckTransElement.getChild(CONTAINER)
                            if (containerElement != null) {
                                Element placardElement = containerElement.getChild(PLACARDS)
                                if (placardElement != null) {
                                    List plList = placardElement.getChildren(PLACARD)
                                    String ctrNbr = containerElement.getAttributeValue(EQ_ID)
                                    if (ctrNbr != null) {
                                        String tranType = truckTransElement?.getAttributeValue(TRAN_TYPE)
                                        TranSubTypeEnum tranEnum = TranSubTypeEnum.getEnum(tranType)
                                        TruckTransaction truckTransaction = findTranWithCtr(ctrNbr, tranEnum)
                                        if (truckTransaction != null) {
                                            UnitFinder unitFinder = Roastery.getBean(UnitFinder.BEAN_ID)
                                            Complex complex = Complex.findComplex(COMPLEX, Operator.findOperator(OPERATOR))
                                            Unit unit = unitFinder.findActiveUnit(complex, Equipment.findEquipment(truckTransaction.getTranCtrNbr()))
                                            Set placardSet = getPlacards(unit)
                                            if (plList != null && !plList.isEmpty()) {
                                                for (Element placards : plList) {
                                                    if (placards != null) {
                                                        if (truckTransaction != null) {
                                                            String placard = placards?.getAttributeValue(TEXT)
                                                            if (placard != null) {
                                                                placardSet.add(placard)
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            for (String placard : placardSet) {
                                                if (!truckTransaction.getTranPlacards().contains(placard)) {
                                                    truckTransaction.getTranPlacards().add(TransactionPlacard.create(truckTransaction, Placard?.findPlacard(placard)))
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        //
        log.warn("GateWebservicePreInvokeInTx executing...")
        // Element element = (Element) parameter.get(ArgoGroovyUtils.WS_ROOT_ELEMENT);
        log.warn("element " + element.toString())
        Element messagesElement = new Element("Messages")
        Element processTruckElement = element.getChild(PROCESS_TRUCK) != null ?
                (Element) element.getChild(PROCESS_TRUCK) : null;

        log.warn("process truck element " + processTruckElement.toString())
        if (processTruckElement != null) {
            Element stageIdElement = processTruckElement.getChild(STAGE_ID) != null ?
                    (Element) processTruckElement.getChild(STAGE_ID) : null;
            log.warn("stageId element " + stageIdElement)
            if (stageIdElement != null) {
                if (OUTGATE.equalsIgnoreCase(stageIdElement.getText())) {
                    Element equipmentElement = processTruckElement.getChild(EQUIPMENT)
                    if (equipmentElement != null) {
                        Element containerElement = equipmentElement.getChild(CONTAINER)
                        if (containerElement != null) {
                            String eqId = containerElement.getAttributeValue(EQ_ID)
                            log.warn("eqId " + eqId)
                            if (eqId != null) {
                                processErrorMessage(parameter, processTruckElement, PROCESS_TRUCK_RESPONSE_ELEMENT)
                                Container ctr = Container.findContainerByFullId(eqId)
                                log.warn("ctr " + ctr)
                                if (ctr == null) {
                                    /* Element messageElement = new Element("Message")
                                     PropertyKey propertyKey = PropertyKeyFactory.valueOf("INVALID_CONTAINER_AT_OUTGATE")
                                     messageElement.setAttribute("key", propertyKey.getKey())
                                     messageElement.setAttribute("id", "Truck visit Appointment is not available for Truck ")
                                     messageElement.setAttribute("level", "SEVERE ")
                                     messagesElement.addContent(messageElement)*/
                                    MessageCollectorFactory.createMessageCollector().appendMessage(MessageLevel.SEVERE, PropertyKeyFactory.valueOf("INVALID_CONTAINER_AT_OUTGATE"), null, eqId)
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    private TruckTransaction findTranWithCtr(String inContainerNumber, TranSubTypeEnum inTranEnum) {
        DomainQuery dq = QueryUtils.createDomainQuery(RoadEntity.TRUCK_TRANSACTION)
                .addDqPredicate(PredicateFactory.eq(RoadField.TRAN_CTR_NBR, inContainerNumber))
                .addDqPredicate(PredicateFactory.eq(RoadField.TRAN_SUB_TYPE, inTranEnum))
                .addDqPredicate(PredicateFactory.in(RoadField.TRAN_STATUS, [TranStatusEnum.OK, TranStatusEnum.TROUBLE]))
                .addDqOrdering(Ordering.desc(RoadField.TRAN_CREATED))
        List<TruckTransaction> transactions = (List<TruckTransaction>) HibernateApi.getInstance().findEntitiesByDomainQuery(dq);
        return (transactions && transactions.size() > 0) ? transactions.get(0) : null;
    }

    private Set getPlacards(Unit unit) {
        Set placardset = new HashSet()
        List<HazardItem> hzItemList = unit?.getUnitGoods()?.getGdsHazards()?.getHzrdItems()
        Map<Placard, Boolean> placards = null
        if (hzItemList != null && !hzItemList.isEmpty()) {
            for (HazardItem hazardItem : hzItemList) {
                placards = hazardItem.getPlacards()
                if (placards != null && !placards.isEmpty()) {
                    for (Placard placard : placards.keySet()) {
                        placardset.add(placard.getPlacardText())
                    }
                }
            }
        }
        EqBaseOrder eqBaseOrder = unit?.getDepartureOrder()
        EquipmentOrder equipmentOrder = EquipmentOrder.resolveEqoFromEqbo(eqBaseOrder)
        Booking booking = Booking.hydrate(equipmentOrder.getEqboGkey())
        placards = booking?.getEqoHazards()?.getPlacards()
        if (placards != null && !placards.isEmpty()) {
            for (Placard Unitplacard : placards.keySet()) {
                placardset.add(Unitplacard.getPlacardText())
            }
        }
        return placardset
    }


    void processErrorMessage(Map parameter, Element processTruckElement, String responseElement) {
        log.warn("processErrorMessage executing...")
        log.warn("parameter " + parameter)
        Object[] obj = parameter.get("ARGO_WS_RESULT_HOLDER")
        QueryResultType[] queryResultType = (QueryResultType[]) obj[0]
        QueryResultType type = (QueryResultType) queryResultType[0]
        String result = type.getResult()

        String tvKey = processTruckElement.getChild("truck-visit")?.getAttributeValue("tv-key")
        if (StringUtils.isNotBlank(tvKey)) {
            log.warn("Inside Truck License")
            TruckVisitDetails truckVisitDetails = TruckVisitDetails.findTruckVisitByGkey(Long.parseLong(tvKey))
            log.warn("truck visit key "+tvKey)
            log.warn("truck visit details " + truckVisitDetails)
            if (truckVisitDetails != null) {
                log.warn("Inside Truck Visit Details")
                Document document = (Document) XmlUtil.parse(result)
                log.warn("document " + document)
                Element rootElement = document != null ? (Element) document.getRootElement() : null
                log.warn("root element " + rootElement)
                Element tranResponseElement = rootElement != null ? rootElement.getChild(responseElement) : null
                log.warn("tran response element " + tranResponseElement)
                Element truckTransactionsElement = tranResponseElement != null ? tranResponseElement.getChild("truck-transactions") : null
                log.warn("truck transaction element " + truckTransactionsElement)
                List truckTransactionList = truckTransactionsElement != null ? truckTransactionsElement.getChildren("truck-transaction") : null
                log.warn("truck transaction list " + truckTransactionList)
                if (truckTransactionList != null && !truckTransactionList.isEmpty()) {
                    Set truckTrans = truckVisitDetails.getTvdtlsTruckTrans()
                    log.warn("truck trans " + truckTrans)
                    for (Element truckTransElement : truckTransactionList) {
                        Element messagesElement = new Element("messages")
                        String tranNbr = truckTransElement.getAttributeValue("tran-nbr")
                        log.warn("tran number " + tranNbr)
                        TruckTransaction truckTransaction = getTruckTransactionByNbr(truckTrans, Long.valueOf(tranNbr))
                        log.warn("truck transaction "+truckTransaction)
                        if (truckTransaction != null && truckTransaction.getTranStatus() == TranStatusEnum.COMPLETE) {
                            log.warn("Inside Truck Transaction - " + truckTransaction.getTranNbr().toString())
                            Element messageElement = new Element("Message")
                            PropertyKey propertyKey = PropertyKeyFactory.valueOf("INVALID_CONTAINER_AT_OUTGATE")
                            messageElement.setAttribute("key", propertyKey.getKey())
                            messageElement.setAttribute("id", "Container {container number} does not match with master container")
                            messageElement.setAttribute("level", "SEVERE ")
                            messagesElement.addContent(messageElement)
                        /*    List<DocumentMessage> documentMessageList = getDocumentMessages(truckTransaction)
                            log.warn("document list "+documentMessageList)
                            if (documentMessageList != null && !documentMessageList.isEmpty()) {
                                for (DocumentMessage documentMessage : documentMessageList) {
                                    log.warn("document message " + documentMessage)
                                 *//*   Element messageElement = new Element("message")
                                    if (documentMessage != null && documentMessage.getDocmsgSeverity() == "INFO") {
                                        String docmsgMsgId = documentMessage.getDocmsgMsgId()
                                        String docmsgMsgText = documentMessage.getDocmsgMsgText()
                                        String docmsgSeverity = documentMessage.getDocmsgSeverity()

                                        messageElement.setAttribute("message-id", docmsgMsgId)
                                        messageElement.setAttribute("message-text", docmsgMsgText)
                                        messageElement.setAttribute("message-severity", docmsgSeverity)
                                        messagesElement.addContent(messageElement)
                                        log.warn("Added message " + docmsgMsgText)
                                    }*//*
                                   *//* Element messageElement = new Element("Message")
                                    PropertyKey propertyKey = PropertyKeyFactory.valueOf("INVALID_CONTAINER_AT_OUTGATE")
                                    messageElement.setAttribute("key", propertyKey.getKey())
                                    messageElement.setAttribute("id", "Container {container number} does not match with master container")
                                    messageElement.setAttribute("level", "SEVERE ")
                                    messagesElement.addContent(messageElement)*//*
                                }
                            }*/
                        }
                        truckTransElement.addContent(messagesElement)
                        log.warn("Added message to Truck transaction element")
                    }
                }
                type.setResult(XmlUtil.toString(rootElement, false));
                parameter.put("ARGO_WS_RESULT_HOLDER", obj[0])
            }
        }
    }


    private TruckTransaction getTruckTransactionByNbr(Set truckTrans, Long tranNbr) {
        if (truckTrans != null && !truckTrans.isEmpty()) {
            for (TruckTransaction truckTransaction : truckTrans) {
                if (truckTransaction.getTranNbr() == tranNbr) {
                    return truckTransaction
                }
            }
        }
        return null
    }

    List<DocumentMessage> getDocumentMessages(TruckTransaction tran) {
        List<DocumentMessage> docMsgList = new ArrayList();

        if (TranStatusEnum.OK == tran.getTranStatus() && "outgate" == tran.getTranStageId()) {
            List<com.navis.road.business.model.Document> docList = getDocuments(tran.getPrimaryKey())

            List<Serializable> troubleMsgList = new ArrayList<>()
            if (docList != null && !docList.isEmpty()) {
                for (com.navis.road.business.model.Document document : docList) {
                    Set<DocumentMessage> docMsges = document.getDocMessages()
                    docMsges.each { DocumentMessage it -> troubleMsgList.add(it.getPrimaryKey()) }
                }
            }

            log.warn("troubleMsgList " + troubleMsgList)

            DomainQuery query = QueryUtils.createDomainQuery("DocumentMessage").addDqPredicate(PredicateFactory.eq(RoadField.DOCMSG_TRANSACTION, tran.getTranGkey()))
                    .addDqPredicate(PredicateFactory.eq(RoadField.DOCMSG_SEVERITY, "INFO"))
            if (troubleMsgList != null && !troubleMsgList.isEmpty()) {
                query.addDqPredicate(PredicateFactory.not(PredicateFactory.in(RoadField.DOCMSG_PK, troubleMsgList)))
            }
            log.warn("query " + query)

            return HibernateApi.getInstance().findEntitiesByDomainQuery(query)


        }
        return docMsgList;
    }

    List<com.navis.road.business.model.Document> getDocuments(Serializable tranKey) {
        DomainQuery dq = QueryUtils.createDomainQuery("Document")
                .addDqPredicate(PredicateFactory.eq(RoadField.DOC_TRANSACTION, tranKey))
                .addDqPredicate(PredicateFactory.eq(RoadField.DOC_STAGE_ID, "outgate"))


        DomainQuery docTypeDq = QueryUtils.createDomainQuery("DocumentType")
                .addDqPredicate(PredicateFactory.eq(ArgoField.DOCTYPE_ID, 'COMPLETE'));

        dq.addDqPredicate(PredicateFactory.subQueryIn(docTypeDq, RoadField.DOC_DOC_TYPE))
        dq.addDqOrdering(Ordering.desc(RoadField.DOC_CREATED))
        log.warn("Final Dq -- " + dq)

        return HibernateApi.getInstance().findEntitiesByDomainQuery(dq)

    }
    private final static String COMPLEX = "USLAX"
    private final static String OPERATOR = "PORTSAMERICA"
    private final static String SMTP = "submit-multiple-transactions"
    private final static String TRUCK_VISIT = "truck-visit"
    private final static String TV_KEY = "tv-key"
    private final static String TRUCK_TRANS = "truck-transactions"
    private final static String TRANSACTION = "truck-transaction"
    private final static String CONTAINER = "container"
    private final static String PLACARDS = "placards"
    private final static String PLACARD = "placard"
    private final static String EQ_ID = "eqid"
    private final static String TRAN_TYPE = "tran-type"
    private final static String TEXT = "text"
    private final static String STAGE_ID = "stage-id"
    private final static String PROCESS_TRUCK = "process-truck"
    private final static String EQUIPMENT = "equipment"

    private final static String OUTGATE = "outgate"
    private final String PROCESS_TRUCK_RESPONSE_ELEMENT = "process-truck-response"
    private final static Logger log = Logger.getLogger(GateWebservicePostInvokeInTx.class)
}