/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.cantor.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.salesforce.cantor.Cantor;
import com.salesforce.cantor.Events;
import com.salesforce.cantor.Maps;
import com.salesforce.cantor.Objects;
import com.salesforce.cantor.Sets;

import java.io.IOException;

public class CantorOnS3 implements Cantor {
    private final Objects objects;

    public CantorOnS3(final AmazonS3 s3Client) throws IOException {
        this.objects = new ObjectsOnS3(s3Client);
    }

    @Override
    public Objects objects() {
        return this.objects;
    }

    @Override
    public Sets sets() {
        throw new UnsupportedOperationException("Sets not implemented on S3");
    }

    @Override
    public Maps maps() {
        throw new UnsupportedOperationException("Sets not implemented on S3");
    }

    @Override
    public Events events() {
        throw new UnsupportedOperationException("Events not implemented on S3");
    }
}
