/**
 * 
 */
package com.informatica.eic.scanner;

import io.swagger.parser.v3.OpenAPIV3Parser;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.opencsv.CSVWriter;

import io.swagger.oas.models.ExternalDocumentation;
import io.swagger.oas.models.OpenAPI;
import io.swagger.oas.models.Operation;
import io.swagger.oas.models.PathItem;
import io.swagger.oas.models.media.Schema;
import io.swagger.oas.models.parameters.Parameter;
import io.swagger.oas.models.responses.ApiResponse;
import io.swagger.oas.models.responses.ApiResponses;
import io.swagger.oas.models.tags.Tag;

/**
 * @author Gaurav
 *
 */
public class SwaggerParserTest {
	
	private static String COLON=":";
	private static String SLASH="/";
	
	

	public static void writeCSVFile(String fileName, String[] header, List<String[]> lines) {
		String swaggercsv = fileName;
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new FileWriter(swaggercsv));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		writer.writeNext(header);

		for (String[] nextLine : lines) {
			writer.writeNext(nextLine);
		}

		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// read a swagger description from the petstore
		// OpenAPI swagger = new
		// OpenAPIV3Parser().read("http://petstore.swagger.io/v2/swagger.json");
		OpenAPI swagger = new OpenAPIV3Parser().read("swagger.json");
		
		if(swagger==null) {
			System.err.println("Unable to read the swagger location");
			System.exit(-1);
		}

		System.out.println(swagger.getInfo().getTitle());
		System.out.println(swagger.getInfo().getDescription());
		System.out.println(swagger.getInfo().getVersion());
		System.out.println(swagger.getInfo().getContact()!=null ? swagger.getInfo().getContact().getEmail() : "");
		System.out.println("==========");

		String swaggerHeader = "class,identity,core.name,core.description,com.ldm.custom.swaggerapiv2.email,com.ldm.custom.swaggerapiv2.version";

		String[] line = new String[] { "com.ldm.custom.swaggerapiv2.SwaggerInfo",
				createEICID(swagger.getInfo().getTitle()), swagger.getInfo().getTitle(),
				swagger.getInfo().getDescription(), swagger.getInfo().getContact()!=null ? swagger.getInfo().getContact().getEmail() : "",
				swagger.getInfo().getVersion() };

		ArrayList<String[]> lines = new ArrayList<String[]>();
		lines.add(line);
		writeCSVFile("swagger.csv", swaggerHeader.split(","), lines);

		// System.out.println(swagger.getOpenapi());
		//
		// Components components=swagger.getComponents();
		//
		// for(String schema: components.getSchemas().keySet()) {
		// System.out.println(schema);
		// Schema s=components.getSchemas().get(schema);
		// }
		//
		//
		// System.out.println("==========");

		// String endpointcsv="Endpoint.csv";
		// CSVWriter writer = null;
		// try {
		// writer = new CSVWriter(new FileWriter(endpointcsv));
		// } catch (IOException e1) {
		// // TODO Auto-generated catch block
		// e1.printStackTrace();
		// }

		String tagHeader = "class,identity,core.name,core.description,com.ldm.custom.swaggerapiv2.url";
		List<Tag> tags = swagger.getTags();
		ArrayList<String[]> tagLines = new ArrayList<String[]>();

		String linkHeader = "association,fromObjectIdentity,toObjectIdentity";

		ArrayList<String[]> linkLines = new ArrayList<String[]>();

		Map<String, Tag> tagMap = new HashMap<String, Tag>();

		if (tags != null) {
			for (Tag tag : tags) {
				System.out.println(tag.getName());
				tagMap.put(tag.getName(), tag);
				System.out.println(tag.getDescription());
				ExternalDocumentation tagdocs = tag.getExternalDocs();
				if (tagdocs != null) {
					System.out.println(tagdocs.getUrl());
				}

				System.out.println("==========");
			}

		}

		String endpointHeader = "class,identity,core.name,core.description,com.ldm.custom.swaggerapiv2.summary";
		ArrayList<String[]> endpointLines = new ArrayList<String[]>();

		String paramHeader = "class,identity,core.name,core.description,com.ldm.custom.swaggerapiv2.in,com.ldm.custom.swaggerapiv2.required,com.ldm.custom.swaggerapiv2.type";
		ArrayList<String[]> paramLines = new ArrayList<String[]>();

		String responseHeader = "class,identity,core.name,core.description";
		ArrayList<String[]> responseLines = new ArrayList<String[]>();

		Set<String> endpoints = swagger.getPaths().keySet();

		for (String endpoint : endpoints) {
			PathItem item = swagger.getPaths().get(endpoint);

			Map<String, Operation> operations = new HashMap<String, Operation>();

			if (item.getGet() != null) {
				String opType = "[GET]";
				operations.put(opType + COLON + endpoint, item.getGet());
			}
			if (item.getPost() != null) {
				String opType = "[POST]";
				operations.put(opType + COLON + endpoint, item.getPost());
			}
			if (item.getPut() != null) {
				String opType = "[PUT]";
				operations.put(opType + COLON + endpoint, item.getPut());
			}
			if (item.getDelete() != null) {
				String opType = "[DELETE]";
				operations.put(opType + COLON + endpoint, item.getDelete());
			}

			for (String opName : operations.keySet()) {
				Operation operation = operations.get(opName);
				List<String> opTags = operation.getTags();
				String tagName = "NO_TAG";

				for (String opTag : opTags) {
					System.out.println("Tag: " + opTag);

					if (!tagMap.containsKey(opTag)) {
						Tag ntag = new Tag();
						ntag.setName(opTag);
						tagMap.put(opTag, ntag);

					}
					// TODO: Wrong, assumes only one tag per operation
					tagName = opTag;

					linkLines.add(new String[] { "com.ldm.custom.swaggerapiv2.TagEndpoint",
							createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName),
							createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
									+ createEICID(opName), });

				}

				endpointLines.add(new String[] { "com.ldm.custom.swaggerapiv2.Endpoint",
						createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
								+ createEICID(opName),
						opName, operation.getDescription() == null ? "" : operation.getDescription(),
						operation.getSummary() == null ? "" : operation.getSummary() });

				System.out.println(opName);
				System.out.println(operation.getDescription() == null ? "" : operation.getDescription());
				System.out.println(operation.getSummary() == null ? "" : operation.getSummary());

				if (operation.getParameters() != null) {

					for (Parameter param : operation.getParameters()) {

						paramLines.add(new String[] { "com.ldm.custom.swaggerapiv2.Parameter",
								createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
										+ createEICID(opName) + SLASH + createEICID(param.getName()),
								param.getName(), param.getDescription(), param.getIn(),
								param.getRequired() != null ? param.getRequired() + "" : "false",
								param.getSchema().getType() });

						linkLines.add(new String[] { "com.ldm.custom.swaggerapiv2.EndpointParameter",
								createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
										+ createEICID(opName),
								createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
										+ createEICID(opName) + SLASH + createEICID(param.getName()), });

						System.out.println(param.getName());
						System.out.println(param.getDescription());
						System.out.println(param.getIn());
						System.out.println(param.getRequired());

						Schema schema = param.getSchema();
						System.out.println(schema.getType());
						System.out.println("==========");
					}
				}

				if (operation.getResponses() != null) {
					ApiResponses responses = operation.getResponses();
					for (String responseName : responses.keySet()) {

						System.out.println(responseName);
						ApiResponse response = responses.get(responseName);

						responseLines.add(new String[] { "com.ldm.custom.swaggerapiv2.Response",
								createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
										+ createEICID(opName) + SLASH + createEICID(responseName),
								responseName, response.getDescription() });
						linkLines.add(new String[] { "com.ldm.custom.swaggerapiv2.EndpointResponse",
								createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
										+ createEICID(opName),
								createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
										+ createEICID(opName) + SLASH + createEICID(responseName), });

						System.out.println(response.getDescription());
						// System.out.println(response.getContent());
						System.out.println("==========");
					}
				}
				System.out.println("==========");
			}

		}

		// Write Tags

		for (String tagN : tagMap.keySet()) {

			Tag tag = tagMap.get(tagN);

			tagLines.add(new String[] { "com.ldm.custom.swaggerapiv2.Tag",
					createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tag.getName()), tag.getName(),
					tag.getDescription() != null ? tag.getDescription() : "",
					tag.getExternalDocs() != null ? tag.getExternalDocs().getUrl() : "" });

			linkLines.add(new String[] { "com.ldm.custom.swaggerapiv2.SwaggerInfoTag",
					createEICID(swagger.getInfo().getTitle()),
					createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tag.getName()),

			});
		}

		writeCSVFile("tags.csv", tagHeader.split(","), tagLines);

		writeCSVFile("Endpoint.csv", endpointHeader.split(","), endpointLines);
		writeCSVFile("Parameters.csv", paramHeader.split(","), paramLines);
		writeCSVFile("Responses.csv", responseHeader.split(","), responseLines);
		writeCSVFile("links.csv", linkHeader.split(","), linkLines);

	}

	public static String createEICID(String name) {
		return name.replace(" ", "_").replace("/", "_").replace("{", "_").replace("}", "_").replace("[", "_")
				.replace("]", "_").replace(":", "_");

	}

}
