/*
 * Lthread
 * Copyright (C) 2012, Hasan Alayli <halayli@gmail.com>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 * lthread_sched.c
 */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>
#include <errno.h>
#include <inttypes.h>

#include "lthread_int.h"
#include "tree.h"

#define FD_KEY(f,e) (((int64_t)(f) << (sizeof(int32_t) * 8)) | e)
#define FD_EVENT(f) ((int32_t)(f))
#define FD_ONLY(f) ((f) >> ((sizeof(int32_t) * 8)))

static inline int _lthread_sleep_cmp(struct lthread *l1, struct lthread *l2);
static inline int _lthread_wait_cmp(struct lthread *l1, struct lthread *l2);

static inline int
_lthread_sleep_cmp(struct lthread *l1, struct lthread *l2)
{
    if (l1->sleep_usecs < l2->sleep_usecs)
        return (-1);
    if (l1->sleep_usecs == l2->sleep_usecs)
        return (0);
    return (1);
}

static inline int
_lthread_wait_cmp(struct lthread *l1, struct lthread *l2)
{
    if (l1->fd_wait < l2->fd_wait)
        return (-1);
    if (l1->fd_wait == l2->fd_wait)
        return (0);
    return (1);
}

RB_GENERATE(lthread_rb_sleep, lthread, sleep_node, _lthread_sleep_cmp);
RB_GENERATE(lthread_rb_wait, lthread, wait_node, _lthread_wait_cmp);

static uint64_t _lthread_min_timeout(struct lthread_sched *);

static int  _lthread_poll(void);
static void _lthread_resume_expired(struct lthread_sched *sched);
static inline int _lthread_sched_isdone(struct lthread_sched *sched);

static struct lthread find_lt;

// 监听事件是否可用
static int
_lthread_poll(void)
{
    struct lthread_sched *sched;
    sched = lthread_get_sched();
    struct timespec t = {0, 0};
    int ret = 0;
    uint64_t usecs = 0;

    sched->num_new_events = 0;
    usecs = _lthread_min_timeout(sched); // 最早超时的睡眠协程

    /* never sleep if we have an lthread pending in the new queue */
    if (usecs && TAILQ_EMPTY(&sched->ready)) {
        t.tv_sec =  usecs / 1000000u;
        if (t.tv_sec != 0)
            t.tv_nsec  =  (usecs % 1000u)  * 1000000u;
        else
            t.tv_nsec = usecs * 1000u;
    } else {
        t.tv_nsec = 0;
        t.tv_sec = 0;
    }


    while (1) {
        ret = _lthread_poller_poll(t); // 监听IO是否可读写
        if (ret == -1 && errno == EINTR) {
            continue;
        } else if (ret == -1) {
            perror("error adding events to epoll/kqueue");
            assert(0);
        }
        break;
    }

    sched->nevents = 0;
    sched->num_new_events = ret; // 可读写的fd数量

    return (0);
}

// 找到最早超时的睡眠协程
static uint64_t
_lthread_min_timeout(struct lthread_sched *sched)
{
    uint64_t t_diff_usecs = 0, min = 0;
    struct lthread *lt = NULL;

    t_diff_usecs = _lthread_diff_usecs(sched->birth,
        _lthread_usec_now());
    min = sched->default_timeout;

    lt = RB_MIN(lthread_rb_sleep, &sched->sleeping);
    if (!lt)
        return (min);

    min = lt->sleep_usecs;
    if (min > t_diff_usecs)
        return (min - t_diff_usecs);
    else // we are running late on a thread, execute immediately
        return (0);

    return (0);
}

/*
 * Returns 0 if there is a pending job in scheduler or 1 if done and can exit.
 */
static inline int
_lthread_sched_isdone(struct lthread_sched *sched)
{
    // 判断是否没有协程在运行:
	// =======================
	// 1) 没有等待的协程
	// 2) 没有繁忙的协程
	// 3) 没有睡眠的协程
	// 4) 没有准备的协程

    return (RB_EMPTY(&sched->waiting) &&
        LIST_EMPTY(&sched->busy) &&
        RB_EMPTY(&sched->sleeping) &&
        TAILQ_EMPTY(&sched->ready));
}

/*
 * 调度器主循环
 */
void
lthread_run(void)
{
    struct lthread_sched *sched;
    struct lthread *lt = NULL, *lt_tmp = NULL;
    struct lthread *lt_read = NULL, *lt_write = NULL;
    int p = 0;
    int fd = 0;
    int is_eof = 0;

    sched = lthread_get_sched();
    /* scheduler not initiliazed, and no lthreads where created */
    if (sched == NULL)
        return;

    while (!_lthread_sched_isdone(sched)) { // 如果没有协程, 那么直接退出

        /* 1. start by checking if a sleeping thread needs to wakeup */
        _lthread_resume_expired(sched); // 唤醒已经超时的协程

        /* 2. check to see if we have any ready threads to run */
        while (!TAILQ_EMPTY(&sched->ready)) { // 从准备队列中找到一个协程
            TAILQ_FOREACH_SAFE(lt, &sched->ready, ready_next, lt_tmp) {
                TAILQ_REMOVE(&lt->sched->ready, lt, ready_next);
                _lthread_resume(lt); // 恢复lt这个协程的运行
            }
        }

        /* 3. resume lthreads we received from lthread_compute, if any */
        while (1) { // 处理那些完成IO的协程
            assert(pthread_mutex_lock(&sched->defer_mutex) == 0);
            lt = TAILQ_FIRST(&sched->defer);
            if (lt == NULL) {
                assert(pthread_mutex_unlock(&sched->defer_mutex) == 0);
                break;
            }
            TAILQ_REMOVE(&sched->defer, lt, defer_next);
            assert(pthread_mutex_unlock(&sched->defer_mutex) == 0);
            LIST_REMOVE(lt, busy_next);
            _lthread_resume(lt); // 恢复运行
        }

        /* 4. check if we received any events after lthread_poll */
        // 监听IO是否可读写
        _lthread_poll();

        /* 5. fire up lthreads that are ready to run */
        while (sched->num_new_events) { // 处理可读写的fd
            p = --sched->num_new_events;

            fd = _lthread_poller_ev_get_fd(&sched->eventlist[p]);

            /* 
             * We got signaled via trigger to wakeup from polling & rusume file io.
             * Those lthreads will get handled in step 4.
             */
            if (fd == sched->eventfd) { // 如果是通知fd, 那么不用处理这个
                _lthread_poller_ev_clear_trigger();
                continue;
            }

            is_eof = _lthread_poller_ev_is_eof(&sched->eventlist[p]);
            if (is_eof)
                errno = ECONNRESET;

            lt_read = _lthread_desched_event(fd, LT_EV_READ);
            if (lt_read != NULL) { // 可读
                if (is_eof)
                    lt_read->state |= BIT(LT_ST_FDEOF);
                _lthread_resume(lt_read);
            }

            lt_write = _lthread_desched_event(fd, LT_EV_WRITE);
            if (lt_write != NULL) { // 可写
                if (is_eof)
                    lt_write->state |= BIT(LT_ST_FDEOF);
                _lthread_resume(lt_write);
            }
            is_eof = 0;

            assert(lt_write != NULL || lt_read != NULL);
        }
    }

    _sched_free(sched);

    return;
}

/*
 * Cancels registered event in poller and deschedules (fd, ev) -> lt from
 * rbtree. This is safe to be called even if the lthread wasn't waiting on an
 * event.
 */
// 清除协程的事件监听
void
_lthread_cancel_event(struct lthread *lt)
{
    if (lt->state & BIT(LT_ST_WAIT_READ)) {
        _lthread_poller_ev_clear_rd(FD_ONLY(lt->fd_wait));
        lt->state &= CLEARBIT(LT_ST_WAIT_READ);

    } else if (lt->state & BIT(LT_ST_WAIT_WRITE)) {
        _lthread_poller_ev_clear_wr(FD_ONLY(lt->fd_wait));
        lt->state &= CLEARBIT(LT_ST_WAIT_WRITE);
    }

    if (lt->fd_wait >= 0)
        _lthread_desched_event(FD_ONLY(lt->fd_wait),
            FD_EVENT(lt->fd_wait));
    lt->fd_wait = -1;
}

/*
 * Deschedules an event by removing the (fd, ev) -> lt node from rbtree.
 * It also deschedules the lthread from sleeping in case it was in sleeping
 * tree.
 */
// 唤醒fd对应协程
struct lthread *
_lthread_desched_event(int fd, enum lthread_event e)
{
    struct lthread *lt = NULL;
    struct lthread_sched *sched = lthread_get_sched();
    find_lt.fd_wait = FD_KEY(fd, e);

    lt = RB_FIND(lthread_rb_wait, &sched->waiting, &find_lt);
    if (lt != NULL) {
        RB_REMOVE(lthread_rb_wait, &lt->sched->waiting, lt); // 从红黑树中删除
        _lthread_desched_sleep(lt);                          // 从sleep队列中删除
    }

    return (lt);
}

/*
 * Schedules an lthread for a poller event.
 * Sets its state to LT_EV_(READ|WRITE) and inserts lthread in waiting rbtree.
 * When the event occurs, the state is cleared and node is removed by 
 * _lthread_desched_event() called from lthread_run().
 *
 * If event doesn't occur and lthread expired waiting, _lthread_cancel_event()
 * must be called.
 */
void
_lthread_sched_event(struct lthread *lt, int fd, enum lthread_event e,
    uint64_t timeout)
{
    struct lthread *lt_tmp = NULL;
    enum lthread_st st;
    if (lt->state & BIT(LT_ST_WAIT_READ) || lt->state & BIT(LT_ST_WAIT_WRITE)) {
        printf("Unexpected event. lt id %"PRIu64" fd %"PRId64" already in %"PRId32" state\n",
            lt->id, lt->fd_wait, lt->state);
        assert(0);
    }

    if (e == LT_EV_READ) {          // 监听读事件
        st = LT_ST_WAIT_READ;
        _lthread_poller_ev_register_rd(fd); // 注册读事件

    } else if (e == LT_EV_WRITE) {  // 监听写事件
        st = LT_ST_WAIT_WRITE;
        _lthread_poller_ev_register_wr(fd); // 注册写事件

    } else {
        assert(0);
    }

    lt->state |= BIT(st);
    lt->fd_wait = FD_KEY(fd, e); // 生成key

    lt_tmp = RB_INSERT(lthread_rb_wait, &lt->sched->waiting, lt); // 插入到等待红黑树中 (这样协程上下文才不会丢失, 当fd可读写的时候会被唤醒)

    assert(lt_tmp == NULL);

    _lthread_sched_sleep(lt, timeout); // 睡眠当前协程

    // 这里会被唤醒

    lt->fd_wait = -1;
    lt->state &= CLEARBIT(st);
}

/*
 * Removes lthread from sleeping rbtree.
 * This can be called multiple times on the same lthread regardless if it was
 * sleeping or not.
 */
void
_lthread_desched_sleep(struct lthread *lt)
{
    if (lt->state & BIT(LT_ST_SLEEPING)) {
        RB_REMOVE(lthread_rb_sleep, &lt->sched->sleeping, lt); // 从红黑树中删除
        lt->state &= CLEARBIT(LT_ST_SLEEPING);                 // 删除睡眠标志
        lt->state |= BIT(LT_ST_READY);                         // 添加准备标志
        lt->state &= CLEARBIT(LT_ST_EXPIRED);                  // 删除超时标志
    }
}

/*
 * Schedules lthread to sleep for `msecs` by inserting lthread into sleeping
 * rbtree and setting the lthread state to LT_ST_SLEEPING.
 * lthread state is cleared upon resumption or expiry.
 */
// 睡眠lt协程
void
_lthread_sched_sleep(struct lthread *lt, uint64_t msecs)
{
    struct lthread *lt_tmp = NULL;
    uint64_t usecs = msecs * 1000u;

    /*
     * if msecs is 0, we won't schedule lthread otherwise loop until
     * collision resolved(very rare) by incrementing usec++.
     */
    lt->sleep_usecs = _lthread_diff_usecs(lt->sched->birth,
        _lthread_usec_now()) + usecs;
    while (msecs) {
        lt_tmp = RB_INSERT(lthread_rb_sleep, &lt->sched->sleeping, lt); // 插入到红黑树中
        if (lt_tmp) {
            lt->sleep_usecs++;
            continue;
        }
        lt->state |= BIT(LT_ST_SLEEPING);
        break;
    }

    _lthread_yield(lt); // 然出CPU

    // 执行到这里代表此协程已经被唤醒
    if (msecs > 0)
        lt->state &= CLEARBIT(LT_ST_SLEEPING);
    lt->sleep_usecs = 0;
}

void
_lthread_sched_busy_sleep(struct lthread *lt, uint64_t msecs)
{

    LIST_INSERT_HEAD(&lt->sched->busy, lt, busy_next); // 插入到忙队列中
    lt->state |= BIT(LT_ST_BUSY);
    _lthread_sched_sleep(lt, msecs); // 睡眠
    lt->state &= CLEARBIT(LT_ST_BUSY);
    LIST_REMOVE(lt, busy_next); // 从忙队列中删除
}

/*
 * Resumes expired lthread and cancels its events whether it was waiting
 * on one or not, and deschedules it from sleeping rbtree in case it was
 * sleeping.
 */
// 唤醒那些睡眠时间完成的协程
static void
_lthread_resume_expired(struct lthread_sched *sched)
{
    struct lthread *lt = NULL;
    //struct lthread *lt_tmp = NULL;
    uint64_t t_diff_usecs = 0;

    /* current scheduler time */
    t_diff_usecs = _lthread_diff_usecs(sched->birth, _lthread_usec_now());

    while (1) {
        lt = RB_MIN(lthread_rb_sleep, &sched->sleeping); // 找到睡眠中最早醒来的协程
        if (lt == NULL)
            break;

        if (lt->sleep_usecs <= t_diff_usecs) { // 协程已经睡眠足够

            _lthread_cancel_event(lt);         // 取消IO事件
            _lthread_desched_sleep(lt);        // 把当前协程从睡眠队列中删除
            lt->state |= BIT(LT_ST_EXPIRED);   // 添加超时标志

            /* don't clear expired if lthread exited/cancelled */
            if (_lthread_resume(lt) != -1) // 恢复此协程的运行
                lt->state &= CLEARBIT(LT_ST_EXPIRED);

            continue;
        }
        break;
    }
}
