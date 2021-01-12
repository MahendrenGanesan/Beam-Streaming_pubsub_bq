package org.bnp.trades;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.PCollection;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.jayway.jsonpath.JsonPath; 

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.*;;

public class EquityStreamParser {
	public static ClassLoader classLoader = EquityStreamParser.class.getClassLoader();
	private static final Logger LOG = LoggerFactory.getLogger(EquityStreamParser.class);
	public static void main(String[] arg) {
		System.out.println("InitCredentials..");
		Credentials credential = InitCredentials();
		System.out.println("InitPipelines..");
		InitPipeline(arg, credential);
		System.out.println("InitPipeline and Credentilas completed.");
	}

	private static Credentials InitCredentials() {
		Credentials credential = null;
		List<String> SCOPES = new ArrayList<>();
		SCOPES.addAll(StorageScopes.all());
		SCOPES.addAll(BigqueryScopes.all());
		InputStream in = classLoader.getResourceAsStream("googleCredentials.json");
		try {
			credential = GoogleCredentials.fromStream(in).createScoped(SCOPES);
		} catch (IOException e) {
			e.printStackTrace();

		}
		return credential;

	}

	private static void InitPipeline(String[] args, Credentials credential) {
		// DataflowPipelineOptions options =
		// PipelineOptionsFactory.fromArgs(args).withValidation().create();

		DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		options.setProject("infinite-hope-294204");
		//options.setTempLocation("gs://portfolio_accounting_staging/templates/FlatFileStreamV0.4");
		//options.setDataflowJobFile("gs://portfolio_accounting_staging/templates/FlatFileStreamV0.4/json");
		//options.setStagingLocation("gs://temp-mahe");
		options.setRunner(DataflowRunner.class);
		options.setStreaming(true);
		options.setGcpCredential(credential);
		options.setRegion("asia-east1");
		options.setZone("australia-southeast1-a");
		Pipeline p = Pipeline.create(options);
		LOG.debug("Pipeline Initiated.");
		// Pipeline p = Pipeline.create(options);
		PCollection<PubsubMessage> pClPubSubMsg = p.apply("readPubSubIO", PubsubIO.readMessagesWithAttributes()
				.fromSubscription("projects/infinite-hope-294204/subscriptions/sub1"));
		LOG.debug("PubSub Initiated.");
		PCollection<TableRow> pCl_Rows = pClPubSubMsg.apply("TransformToTableRow", new PubSubToEntEq());
		LOG.debug("Row Extraction Initiated.");
		
		pCl_Rows.apply("InsertTableRowsToBigQuery",
				BigQueryIO.writeTableRows().to("ds.stagingEq")
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		
		p.run().waitUntilFinish();
		LOG.debug("Finished.");
	}

	public static class PubSubToEntEq extends PTransform<PCollection<PubsubMessage>, PCollection<TableRow>> {

		@Override
		public PCollection<TableRow> expand(PCollection<PubsubMessage> input) {

			return input.apply(ParDo.of(new ParallelEntEq()));
		}
	}

	public static class ParallelEntEq extends DoFn<PubsubMessage, TableRow> {
		@ProcessElement
		public void ProcessElement(ProcessContext pc) {
			LOG.debug("ProcessElement Initiated.");
			PubsubMessage rawmessage = pc.element();
			TableRow tblRow = null;
			if (rawmessage != null && rawmessage.getPayload() != null) {
				Object obj = new Object();
				JSONObject jsonObject = null;
				JSONParser parser = new JSONParser();
				try {
					obj = parser.parse(new String(rawmessage.getPayload(), StandardCharsets.UTF_8));
				} catch (ParseException e) {

					e.printStackTrace();
					LOG.error("Exception."+e.getMessage());
				}
				if (obj instanceof JSONArray) {
					JSONArray array = (JSONArray) obj;
					jsonObject = (JSONObject) array.get(0); // to
															// include
															// more
				}
				if (obj instanceof JSONObject) {
					jsonObject = (JSONObject) obj;
				}

				HashMap<String, Object> tempResult = new HashMap<>();

				/*
				 * Trades:[ { "symbol": "tata", "qty": "-10", "cpty": "c1pf1", "price": 8.95 },
				 * { "symbol": "tata1", "qty": "-10", "cpty": "c1pf1", "price": 8.95 } ]
				 */
				if (jsonObject != null) {
					
					if (JsonPath.read(jsonObject.toString(), "$.trades[1].symbol") != null)
					{
						LOG.debug("Message Found!"+ JsonPath.read(jsonObject.toString(), "$.trades[1].symbol"));
						tempResult.put("symbol", JsonPath.read(jsonObject.toString(), "$.trades[1].symbol"));
					}
					if (JsonPath.read(jsonObject.toString(), "$.trades[1].qty") != null)
						tempResult.put("qty", JsonPath.read(jsonObject.toString(), "$.trades[1].qty"));
					if (JsonPath.read(jsonObject.toString(), "$.trades[1].cpty") != null)
						tempResult.put("cpty", JsonPath.read(jsonObject.toString(), "$.trades[1].cpty"));
					if (JsonPath.read(jsonObject.toString(), "$.trades[1].price") != null)
						tempResult.put("price", JsonPath.read(jsonObject.toString(), "$.trades[1].price"));
				}
				else
				{
					LOG.debug("No Message Found!");
				}
				try {
					ObjectMapper objectmapper = new ObjectMapper();
					objectmapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
					objectmapper.setSerializationInclusion(Include.NON_NULL);
					// objectmapper.readValue(objectmapper.writeValueAsString(tempResult),
					// TableRow.class);

					tblRow = objectmapper.readValue(objectmapper.writeValueAsString(tempResult), TableRow.class);

				} catch (IOException e) {

					e.printStackTrace();
				}

				pc.output(tblRow);
			}

		}

	}

}
