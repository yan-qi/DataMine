/**
 * Copyright (C) 2016 Turn Inc. (yan.qi@turn.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package datamine.storage.idl.validate;

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import datamine.storage.idl.Schema;
import datamine.storage.idl.generator.metadata.MetadataPackageToSchema;
import datamine.storage.idl.json.JsonSchemaConvertor;
import datamine.storage.idl.validate.exceptions.AbstractValidationException;

public class MetadataPackageToSchemaTest {

	private final Schema currentSchema =
			new MetadataPackageToSchema().apply("datamine.storage.recordbuffers.example.model");
	
	@Test
	public void validate() throws IOException, AbstractValidationException {
		File schemaPath = new File("src/test/resources/RBSchema.json");
		Schema nextSchema = new JsonSchemaConvertor().apply(
				Files.toString(schemaPath, Charsets.UTF_8));
		SchemaEvolutionValidation validate =
				new SchemaEvolutionValidation(currentSchema);
		validate.check(nextSchema);
	}
	
	@Test
	public void testLoading() {
		String inputPackageName = 
				"datamine.storage.recordbuffers.example.model";
		Schema schema = new MetadataPackageToSchema().apply(inputPackageName);
		
		Assert.assertEquals(4, schema.getTableList().size());
	}
}
