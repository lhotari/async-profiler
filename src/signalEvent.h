/*
 * Copyright The async-profiler authors
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _SIGNALEVENT_H
#define _SIGNALEVENT_H

#include <pthread.h>
#include <signal.h>
#include "engine.h"


class SignalEvent : public Engine {
  private:
    static int _signal;
    static int _pipe[2];
    static int _output;
    static pthread_t _writer_thread;
    static SigAction _previous_handler;
    static long _interval;
    static volatile u64 _last_sample;
    static volatile u64 _failed_traces;
    static volatile u64 _dropped_samples;

    static void signalHandler(int signo, siginfo_t* siginfo, void* ucontext);
    static void* writerThreadEntry(void* unused);
    static void writerLoop();
    static Error startJsonlWriter(const char* file);
    static void stopJsonlWriter();

  public:
    const char* type() {
        return EVENT_SIGNAL;
    }

    const char* title() {
        return "Externally triggered samples";
    }

    const char* units() {
        return "samples";
    }

    long interval() {
        return _interval;
    }

    Error start(Arguments& args);
    void stop();
};

#endif // _SIGNALEVENT_H
