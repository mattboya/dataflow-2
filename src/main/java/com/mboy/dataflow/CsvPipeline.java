package com.mboy.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CsvPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(CsvPipeline.class);

	/**
	 * The custom options supported by the pipeline. Inherits standard configuration
	 * options.
	 */
	public interface Options extends PipelineOptions {
		@Description("The file pattern to read records from (e.g. gs://bucket/file-*.csv)")
		@Required
		ValueProvider<String> getInputFilePattern();
		void setInputFilePattern(ValueProvider<String> inputFilePattern);

		@Description("BigQuery table (e.g. project:my_dataset.my_table)")
		@Required
		ValueProvider<String> getBigQueryTable();
		void setBigQueryTable(ValueProvider<String> bigQueryTable);
	}
	
	public static void main(String[] args) {
		LOG.info("<><> CSV pipeline");
		// Parse the user options passed from the command-line
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		run(options);	
	}
	
	/**
	 * Executes the pipeline with the provided execution parameters.
	 *
	 * @param options
	 *            The execution parameters.
	 */
	public static PipelineResult run(Options options) {
		LOG.info("<><> run(options)");
		LOG.info("<><> Input file pattern: " + options.getInputFilePattern().get());
		LOG.info("<><> BigQuery table: " + options.getBigQueryTable().get());
		
		// Create the pipeline.
		Pipeline pipeline = Pipeline.create(options);
		String bqTable = options.getBigQueryTable().toString();  //format = project:my_set.my_table
		
		/*
		 * Steps:
		 * 1. Read lines from CSV source
		 * 2. Convert CSV lines to BQ rows
		 * 3. Write BQ rows to BQ table
		 */
//		PCollection<String> csvRows = pipeline.apply(TextIO.read().from(options.getInputFilePattern()));
//		PCollection<TableRow> tableRows = csvRows.apply(ParDo.of(new CsvLineToBQRow()));
//		
//		tableRows
//		.apply(
//			BigQueryIO.writeTableRows()
//			.to(bqTable)
//			.withSchema(getTableSchema())
//			.withWriteDisposition(WriteDisposition.WRITE_APPEND)
//			.withCreateDisposition(CreateDisposition.CREATE_NEVER)
//		);
		
		//v2
		pipeline
		.apply(TextIO.read()
				.from(options.getInputFilePattern()))
//		.apply(ParDo.of(new CsvLineToBQRow()))
//		.apply(BigQueryIO.writeTableRows()
//			.to(bqTable)
//			.withSchema(getTableSchema())
//			.withWriteDisposition(WriteDisposition.WRITE_APPEND)
//			.withCreateDisposition(CreateDisposition.CREATE_NEVER))
		;
		
		return pipeline.run();
	}

}
