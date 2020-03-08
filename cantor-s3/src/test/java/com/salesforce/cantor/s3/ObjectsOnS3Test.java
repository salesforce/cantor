/*
 * Copyright (c) 2020, Salesforce.com, Inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.s3;

import com.adobe.testing.s3mock.testng.S3Mock;
import com.adobe.testing.s3mock.testng.S3MockListener;
import com.amazonaws.services.s3.AmazonS3;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.common.AbstractBaseObjectsTest;
import org.testng.annotations.Listeners;

import java.io.IOException;

@Listeners(value = { S3MockListener.class })
public class ObjectsOnS3Test extends AbstractBaseObjectsTest {

    @Override
    protected double getStoreMagnitude() {
        return 0.25;
    }

    @Override
    protected Cantor getCantor() throws IOException {
        final AmazonS3 s3Client = S3Mock.getInstance().createS3Client("us-west-1");
        return new CantorOnS3(s3Client);
    }
}