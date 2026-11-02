// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_pubnub/otel_pubnub.dart';

/// Traces Pubnub calls with OpenTelemetry.
///
/// `otel_pubnub` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Pubnub call in
/// [tracedPubnubPublish]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'pubnub-example');

  // Wraps a PubNub publish in a PRODUCER span using OTel messaging semconv.
  final pubnubPublishId = await tracedPubnubPublish<String>(
    channel: 'orders',
    payloadSize: 512,
    uuid: 'uu_8f3a2c',
    invoke: () async {
      // Make the Pubnub call here, then report what it returned.
      const id = 'pubnub_8f3a2c';
      return const PubnubCallResult(value: id, timetoken: 'example');
    },
  );
  OTelLog.info(
      'tracedPubnubPublish messaging.message.id returned $pubnubPublishId');

  // Wraps a PubNub subscribe in a CLIENT span.
  final pubnubSubscribeId = await tracedPubnubSubscribe<String>(
    channel: 'orders',
    withPresence: true,
    invoke: () async {
      // Make the Pubnub call here, then report what it returned.
      return 'pubnub_8f3a2c';
    },
  );
  OTelLog.info('tracedPubnubSubscribe returned $pubnubSubscribeId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutPubnubInstrumentationAsync.
  await runWithoutPubnubInstrumentationAsync(
    () => tracedPubnubPublish<String>(
      channel: 'orders',
      payloadSize: 512,
      uuid: 'uu_8f3a2c',
      invoke: () async {
        const id = 'pubnub_8f3a2c';
        return const PubnubCallResult(value: id, timetoken: 'example');
      },
    ),
  );

  await OTel.shutdown();
}
