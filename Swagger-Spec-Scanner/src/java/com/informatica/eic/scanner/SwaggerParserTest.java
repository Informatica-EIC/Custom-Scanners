/**
 *
 */
package com.informatica.eic.scanner;

import io.swagger.parser.SwaggerParser;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.opencsv.CSVWriter;

import io.swagger.models.*;

import io.swagger.models.Swagger;
import io.swagger.models.ExternalDocs;
import io.swagger.models.Operation;
import io.swagger.models.parameters.BodyParameter;
import io.swagger.models.parameters.HeaderParameter;
import io.swagger.models.parameters.QueryParameter;
import io.swagger.models.parameters.PathParameter;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.Path;
import io.swagger.models.Tag;
import io.swagger.models.Response;

import io.swagger.models.parameters.BodyParameter;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.parameters.PathParameter;
import io.swagger.models.properties.ArrayProperty;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.RefProperty;
import io.swagger.models.refs.RefFormat;

import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

/**
 * @author Gaurav
 *
 *
 */
public class SwaggerParserTest {

	private static String COLON = ":";
	private static String SLASH = "/";

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
			System.out.println("File Written/updated: " + fileName);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// switch off log4j Warnings (not using any logging)
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);

		if (args.length < 2) {
			System.out.println(
					"Proper Usage is: java -jar Swagger-Spec-Scanner-Demo-0.1.0-SNAPSHOT.jar -swagger <path_to_swagger_file> -out <output folder>\n\t default output folder is output");
			System.exit(0);
		}

		System.out.println("Swagger Parser Scanner demonstration version");
		printDisplaimer();
		System.out.println("");

		String fileToRead = "";
		String fldrToWrite = "output";
		// simple command-line handler
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase("-swagger")) {
				if (args.length >= i + 2) {
					fileToRead = args[i + 1];
					if (fileToRead.startsWith("-")) {
						System.out.println("no swagger spec file passed after -swagger parameter.  exiting");
						System.exit(0);
					}
				} else {
					System.out.println("no swagger spec file passed after -swagger parameter.  exiting");
					System.exit(0);
				}

			}
			if (args[i].equalsIgnoreCase("-out")) {
				if (args.length >= i + 2) {
					fldrToWrite = args[i + 1];
				} else {
					System.out.println("no folder passed after -out parameter.  exiting");
					System.exit(0);
				}
			}
		}

		System.out.println("starting to parse:  " + fileToRead + " folder=" + fldrToWrite);
		// check that the folder exists - if not, create it
		File directory = new File(String.valueOf(fldrToWrite));
		if (!directory.exists()) {
			System.out.println("\tfolder: " + fldrToWrite + " does not exist, creating it");
			directory.mkdir();
		}

		Swagger swagger = new SwaggerParser().read(fileToRead);

		if (swagger == null) {
			System.err.println("Unable to read the swagger location: " + fileToRead);
			System.exit(-1);
		}

		System.out.println(swagger.getInfo().getTitle());
		System.out.println(swagger.getInfo().getDescription());
		System.out.println(swagger.getInfo().getVersion());
		System.out.println(swagger.getInfo().getContact() != null ? swagger.getInfo().getContact().getEmail() : "");
		System.out.println("==========");

		String swaggerHeader = "class,identity,core.name,core.description,com.ldm.custom.swaggerapiv2.email,com.ldm.custom.swaggerapiv2.version";

		String[] line = new String[] { "com.ldm.custom.swaggerapiv2.SwaggerInfo",
				createEICID(swagger.getInfo().getTitle()), swagger.getInfo().getTitle(),
				swagger.getInfo().getDescription(),
				swagger.getInfo().getContact() != null ? swagger.getInfo().getContact().getEmail() : "",
				swagger.getInfo().getVersion() };

		ArrayList<String[]> swaggerLines = new ArrayList<String[]>();
		swaggerLines.add(line);

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
				ExternalDocs tagdocs = tag.getExternalDocs();
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
			Path item = swagger.getPaths().get(endpoint);

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

						System.out.println("paramter type=" + param.getClass().getSimpleName());
						System.out.println("\tp.getIn()" + param.getIn());
						System.out.println("\tp.getName()" + param.getName());
						System.out.println("\tp.getAccess()" + param.getAccess());
						System.out.println("\tp.getDescription()" + param.getDescription());
						System.out.println("\tp.getPattern()" + param.getPattern());
						System.out.println("\tp.getRequired()" + param.getRequired());

                        // default parameter type (if cannot find)
						String paramType = "string";

						if (param instanceof BodyParameter) {
							BodyParameter bp = (BodyParameter) param;
							System.out.println("bp=" + bp.getSchema().toString());
							// printBody(swagger, bp);
							RefProperty rp = new RefProperty(bp.getSchema().getReference());
							paramType = rp.getSimpleRef();
						}

						if (param instanceof HeaderParameter) {
							HeaderParameter hp = (HeaderParameter) param;
							System.out.println("\theader parm type= " + hp.getType());
							paramType = hp.getType();
							// printHeader(swagger, (HeaderParameter) param);
                        }

                        if (param instanceof QueryParameter) {
                            QueryParameter qp = (QueryParameter) param;
                            System.out.println("\tquery parm type= " + qp.getType());
                            paramType = qp.getType();
                        }

                        if (param instanceof PathParameter) {
                            PathParameter pp = (PathParameter) param;
                            System.out.println("\tpath parm type= " + pp.getType());
                            paramType = pp.getType();
                        }


						paramLines.add(new String[] { "com.ldm.custom.swaggerapiv2.Parameter",
								createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
										+ createEICID(opName) + SLASH + createEICID(param.getName()),
								param.getName(), param.getDescription(), param.getIn(),
								Boolean.toString(param.getRequired()), paramType });

						linkLines.add(new String[] { "com.ldm.custom.swaggerapiv2.EndpointParameter",
								createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
										+ createEICID(opName),
								createEICID(swagger.getInfo().getTitle()) + SLASH + createEICID(tagName) + SLASH
										+ createEICID(opName) + SLASH + createEICID(param.getName()), });

						System.out.println(param.getName());
						System.out.println(param.getDescription());
						System.out.println(param.getIn());
						System.out.println(param.getRequired());

						// Schema schema = param.getSchema();
						System.out.println(param.getClass().getSimpleName());
						System.out.println(paramType);
						System.out.println("=====<>=====");
					}
				}

				if (operation.getResponses() != null) {
					Map<String, Response> responses = operation.getResponses();
					// for (Map.Entry<String, Response> response : responses.entrySet()) {
					for (String responseName : responses.keySet()) {

						System.out.println(responseName);
						Response response = responses.get(responseName);

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

		writeCSVFile(fldrToWrite + "/objects-swagger.csv", swaggerHeader.split(","), swaggerLines);
		writeCSVFile(fldrToWrite + "/objects-tags.csv", tagHeader.split(","), tagLines);

		writeCSVFile(fldrToWrite + "/objects-Endpoint.csv", endpointHeader.split(","), endpointLines);
		writeCSVFile(fldrToWrite + "/objects-Parameters.csv", paramHeader.split(","), paramLines);
		writeCSVFile(fldrToWrite + "/objects-Responses.csv", responseHeader.split(","), responseLines);
		writeCSVFile(fldrToWrite + "/links.csv", linkHeader.split(","), linkLines);

		zipFiles(fldrToWrite);
		System.out.println("");
		System.out.println("Scan complete");

	}

	public static void zipFiles(String fldrToWrite) {
		List<String> srcFiles = new ArrayList<String>();
		srcFiles.add(fldrToWrite + "/objects-swagger.csv");
		srcFiles.add(fldrToWrite + "/objects-tags.csv");
		srcFiles.add(fldrToWrite + "/objects-Endpoint.csv");
		srcFiles.add(fldrToWrite + "/objects-Parameters.csv");
		srcFiles.add(fldrToWrite + "/objects-Responses.csv");
		srcFiles.add(fldrToWrite + "/links.csv");

		try {
			System.out.println("creating zip file: " + fldrToWrite + "/edc-swagger-scanner-result.zip");
			FileOutputStream fos = new FileOutputStream(fldrToWrite + "/edc-swagger-scanner-result.zip");
			ZipOutputStream zipOut = new ZipOutputStream(fos);
			for (String srcFile : srcFiles) {
				File fileToZip = new File(srcFile);
				FileInputStream fis;
				fis = new FileInputStream(fileToZip);
				ZipEntry zipEntry = new ZipEntry(fileToZip.getName());
				zipOut.putNextEntry(zipEntry);

				byte[] bytes = new byte[1024];
				int length;
				while ((length = fis.read(bytes)) >= 0) {
					zipOut.write(bytes, 0, length);
				}
				fis.close();
			}
			zipOut.close();
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static String createEICID(String name) {
		return name.replace(" ", "_").replace("/", "_").replace("{", "_").replace("}", "_").replace("[", "_")
				.replace("]", "_").replace(":", "_");

	}

	// private static void printHeader(Swagger swagger, HeaderParameter hp) {
	// System.out.println("HEADER: ");

	// RefProperty rp = new RefProperty(hp.getType());
	// printReference(swagger, rp);
	// }

	// private static void printBody(Swagger swagger, BodyParameter p) {
	// System.out.println("BODY: ");

	// RefProperty rp = new RefProperty(p.getSchema().getReference());
	// printReference(swagger, rp);
	// }

	// private static void printReference(Swagger swagger, RefProperty rp) {
	// System.out.println("printRference... " + rp.getSimpleRef());
	// if (rp.getRefFormat().equals(RefFormat.INTERNAL) &&
	// swagger.getDefinitions().containsKey(rp.getSimpleRef())) {
	// Model m = swagger.getDefinitions().get(rp.getSimpleRef());
	// System.out.println("\t\tmodel = " + m);
	// if (m instanceof ArrayModel) {
	// ArrayModel arrayModel = (ArrayModel) m;
	// System.out.println(rp.getSimpleRef() + "[]");
	// if (arrayModel.getItems() instanceof RefProperty) {
	// RefProperty arrayModelRefProp = (RefProperty) arrayModel.getItems();
	// printReference(swagger, arrayModelRefProp);
	// }
	// }

	// if (m.getProperties() != null) {
	// for (Map.Entry<String, Property> propertyEntry :
	// m.getProperties().entrySet()) {
	// System.out.println(" " + propertyEntry + " >>>" + propertyEntry.getKey() + "
	// : "
	// + propertyEntry.getValue().getType());
	// }
	// }
	// }
	// }

	private static void printDisplaimer() {
		System.out.println(
				"****************************************************************************************************************************");
		System.out.println("                                                   Disclaimer");
		System.out.println(
				"****************************************************************************************************************************");
		System.out.println(
				"this scanner is not an official product & is not supported by Informatica R&D or GCS (don't create a ticket with GCS)");
		System.out.println(
				"to use it - you will need to build from source (see Build from Source section  in readme.md in github)");
		System.out.println("\t\thttps://github.com/Informatica-EIC/Custom-Scanners/tree/master/Swagger-Spec-Scanner");
		System.out.println("if you find an problem or want to make a comment, you can create a issue on github");
		System.out.println("this scanner is not a 100% coverage of the open-api spec");
		System.out.println(
				"\texample:  the structure of responses for endpoints is not created/documented.  this could be extended (see next point)");
		System.out.println(
				"if you want to extend the model/scanner coverage - please feel free to fork/clone the github repository. ");
		System.out.println(
				"if you improve the scanner - then submit a pull request to update it here for all to benefit");
		System.out.println(
				"****************************************************************************************************************************");

	}
}
