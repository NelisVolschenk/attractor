//! Human-in-the-loop interviewer interface.
//!
//! All human interaction in Attractor goes through the [`Interviewer`] trait.
//! This abstraction allows the pipeline to present questions through any
//! frontend — CLI, web UI, Slack, or a programmatic queue for testing.
//!
//! Built-in implementations:
//! - [`AutoApproveInterviewer`] — always selects YES / first option (CI/CD)
//! - [`QueueInterviewer`] — reads from a pre-filled queue (deterministic testing)

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Question model
// ---------------------------------------------------------------------------

/// The kind of question being asked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuestionType {
    /// Binary yes/no question.
    YesNo,
    /// Select one option from a list.
    MultipleChoice,
    /// Free text input.
    Freeform,
    /// Yes/no confirmation (semantically distinct from `YesNo`).
    Confirmation,
}

/// One selectable option in a [`QuestionType::MultipleChoice`] question.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuestionOption {
    /// Accelerator key (e.g. `"Y"`, `"A"`, `"1"`).
    pub key: String,
    /// Display label (e.g. `"Yes, deploy to production"`).
    pub label: String,
}

/// A question presented to a human via the [`Interviewer`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Question {
    /// The question text to display.
    pub text: String,
    /// The kind of input expected.
    pub question_type: QuestionType,
    /// Options for [`QuestionType::MultipleChoice`] questions.
    pub options: Vec<QuestionOption>,
    /// Default answer if no response is received within `timeout`.
    pub default: Option<Answer>,
    /// Maximum wait time.  `None` means wait indefinitely.
    pub timeout: Option<Duration>,
    /// ID of the stage that generated this question (for display / logging).
    pub stage: String,
    /// Arbitrary metadata key-value pairs.
    pub metadata: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// Answer model
// ---------------------------------------------------------------------------

/// The selected answer value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnswerValue {
    /// Affirmative answer.
    Yes,
    /// Negative answer.
    No,
    /// Human explicitly skipped the question.
    Skipped,
    /// No response received within the timeout.
    Timeout,
    /// A specific option was selected (holds the option key).
    Selected(String),
}

/// Answer returned by an [`Interviewer`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Answer {
    /// The abstract answer value.
    pub value: AnswerValue,
    /// The full selected option (populated for [`QuestionType::MultipleChoice`]).
    pub selected_option: Option<QuestionOption>,
    /// Free-text representation of the answer.
    pub text: String,
}

impl Answer {
    /// Affirmative answer.
    pub fn yes() -> Self {
        Answer {
            value: AnswerValue::Yes,
            selected_option: None,
            text: "yes".to_string(),
        }
    }

    /// Negative answer.
    pub fn no() -> Self {
        Answer {
            value: AnswerValue::No,
            selected_option: None,
            text: "no".to_string(),
        }
    }

    /// Skipped — human chose not to answer.
    pub fn skipped() -> Self {
        Answer {
            value: AnswerValue::Skipped,
            selected_option: None,
            text: String::new(),
        }
    }

    /// Timeout — no response within the allowed time.
    pub fn timeout() -> Self {
        Answer {
            value: AnswerValue::Timeout,
            selected_option: None,
            text: String::new(),
        }
    }

    /// A specific option was selected.
    pub fn selected(option: QuestionOption) -> Self {
        Answer {
            value: AnswerValue::Selected(option.key.clone()),
            text: option.label.clone(),
            selected_option: Some(option),
        }
    }
}

// ---------------------------------------------------------------------------
// Interviewer trait
// ---------------------------------------------------------------------------

/// Human-in-the-loop interface.
///
/// Implementations handle the presentation layer — CLI prompts, web UI,
/// Slack messages, or programmatic queues for testing.
#[async_trait]
pub trait Interviewer: Send + Sync {
    /// Present a question and wait for an answer.
    async fn ask(&self, question: Question) -> Answer;

    /// Present multiple questions in sequence and collect answers.
    async fn ask_multiple(&self, questions: Vec<Question>) -> Vec<Answer> {
        let mut answers = Vec::with_capacity(questions.len());
        for q in questions {
            answers.push(self.ask(q).await);
        }
        answers
    }

    /// Inform the human of a message without requiring an answer.
    async fn inform(&self, _message: &str, _stage: &str) {}
}

// ---------------------------------------------------------------------------
// AutoApproveInterviewer
// ---------------------------------------------------------------------------

/// Always selects YES / the first option.
///
/// Used for CI/CD pipelines and automated tests where no human is present.
pub struct AutoApproveInterviewer;

#[async_trait]
impl Interviewer for AutoApproveInterviewer {
    async fn ask(&self, question: Question) -> Answer {
        match question.question_type {
            QuestionType::YesNo | QuestionType::Confirmation => Answer::yes(),
            QuestionType::MultipleChoice => {
                if let Some(first) = question.options.into_iter().next() {
                    Answer::selected(first)
                } else {
                    Answer {
                        value: AnswerValue::Selected("auto-approved".to_string()),
                        selected_option: None,
                        text: "auto-approved".to_string(),
                    }
                }
            }
            QuestionType::Freeform => Answer {
                value: AnswerValue::Selected("auto-approved".to_string()),
                selected_option: None,
                text: "auto-approved".to_string(),
            },
        }
    }

    async fn inform(&self, _message: &str, _stage: &str) {}
}

// ---------------------------------------------------------------------------
// QueueInterviewer
// ---------------------------------------------------------------------------

/// Reads answers from a pre-filled queue.
///
/// Returns [`Answer::skipped`] when the queue is empty.  Used for
/// deterministic testing and scenario replay.
pub struct QueueInterviewer {
    answers: Mutex<VecDeque<Answer>>,
}

impl QueueInterviewer {
    /// Create a new queue interviewer with the provided answers.
    pub fn new(answers: impl IntoIterator<Item = Answer>) -> Self {
        QueueInterviewer {
            answers: Mutex::new(answers.into_iter().collect()),
        }
    }

    /// Push an additional answer to the back of the queue.
    pub fn push(&self, answer: Answer) {
        self.answers
            .lock()
            .expect("queue interviewer lock poisoned")
            .push_back(answer);
    }

    /// Return the number of answers remaining in the queue.
    pub fn remaining(&self) -> usize {
        self.answers
            .lock()
            .expect("queue interviewer lock poisoned")
            .len()
    }
}

#[async_trait]
impl Interviewer for QueueInterviewer {
    async fn ask(&self, _question: Question) -> Answer {
        self.answers
            .lock()
            .expect("queue interviewer lock poisoned")
            .pop_front()
            .unwrap_or_else(Answer::skipped)
    }

    async fn inform(&self, _message: &str, _stage: &str) {}
}

// ---------------------------------------------------------------------------
// CallbackInterviewer
// ---------------------------------------------------------------------------

/// Delegates question answering to an async callback function.
///
/// Useful for integrating with external systems (Slack, web UI, API) where
/// the answering logic is provided at construction time.
pub struct CallbackInterviewer<F>
where
    F: Fn(Question) -> std::pin::Pin<Box<dyn std::future::Future<Output = Answer> + Send>>
        + Send
        + Sync,
{
    callback: F,
}

impl<F> CallbackInterviewer<F>
where
    F: Fn(Question) -> std::pin::Pin<Box<dyn std::future::Future<Output = Answer> + Send>>
        + Send
        + Sync,
{
    /// Create a new callback interviewer with the provided async function.
    pub fn new(callback: F) -> Self {
        CallbackInterviewer { callback }
    }
}

#[async_trait]
impl<F> Interviewer for CallbackInterviewer<F>
where
    F: Fn(Question) -> std::pin::Pin<Box<dyn std::future::Future<Output = Answer> + Send>>
        + Send
        + Sync,
{
    async fn ask(&self, question: Question) -> Answer {
        (self.callback)(question).await
    }
}

// ---------------------------------------------------------------------------
// RecordingInterviewer
// ---------------------------------------------------------------------------

/// A recorded question-answer pair.
#[derive(Debug, Clone)]
pub struct Recording {
    pub question: Question,
    pub answer: Answer,
}

/// Wraps another [`Interviewer`] and records all question-answer interactions.
///
/// Useful for replay, debugging, and audit trails.
pub struct RecordingInterviewer<I: Interviewer> {
    inner: I,
    recordings: Mutex<Vec<Recording>>,
}

impl<I: Interviewer> RecordingInterviewer<I> {
    /// Create a new recording interviewer wrapping `inner`.
    pub fn new(inner: I) -> Self {
        RecordingInterviewer {
            inner,
            recordings: Mutex::new(Vec::new()),
        }
    }

    /// Return a snapshot of all recorded interactions.
    pub fn recordings(&self) -> Vec<Recording> {
        self.recordings
            .lock()
            .expect("recording interviewer lock poisoned")
            .clone()
    }

    /// Return the number of recorded interactions.
    pub fn recording_count(&self) -> usize {
        self.recordings
            .lock()
            .expect("recording interviewer lock poisoned")
            .len()
    }
}

#[async_trait]
impl<I: Interviewer + Send + Sync> Interviewer for RecordingInterviewer<I> {
    async fn ask(&self, question: Question) -> Answer {
        let answer = self.inner.ask(question.clone()).await;
        self.recordings
            .lock()
            .expect("recording interviewer lock poisoned")
            .push(Recording {
                question,
                answer: answer.clone(),
            });
        answer
    }

    async fn inform(&self, message: &str, stage: &str) {
        self.inner.inform(message, stage).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_mc_question(options: Vec<(&str, &str)>) -> Question {
        Question {
            text: "Choose".to_string(),
            question_type: QuestionType::MultipleChoice,
            options: options
                .into_iter()
                .map(|(k, l)| QuestionOption {
                    key: k.to_string(),
                    label: l.to_string(),
                })
                .collect(),
            default: None,
            timeout: None,
            stage: "test".to_string(),
            metadata: HashMap::new(),
        }
    }

    fn make_yesno_question() -> Question {
        Question {
            text: "Proceed?".to_string(),
            question_type: QuestionType::YesNo,
            options: vec![],
            default: None,
            timeout: None,
            stage: "test".to_string(),
            metadata: HashMap::new(),
        }
    }

    // -- Answer constructors --

    #[test]
    fn answer_yes() {
        let a = Answer::yes();
        assert_eq!(a.value, AnswerValue::Yes);
        assert_eq!(a.text, "yes");
    }

    #[test]
    fn answer_no() {
        let a = Answer::no();
        assert_eq!(a.value, AnswerValue::No);
    }

    #[test]
    fn answer_skipped() {
        let a = Answer::skipped();
        assert_eq!(a.value, AnswerValue::Skipped);
        assert!(a.text.is_empty());
    }

    #[test]
    fn answer_timeout() {
        let a = Answer::timeout();
        assert_eq!(a.value, AnswerValue::Timeout);
    }

    #[test]
    fn answer_selected() {
        let opt = QuestionOption {
            key: "Y".to_string(),
            label: "Yes, deploy".to_string(),
        };
        let a = Answer::selected(opt.clone());
        assert_eq!(a.value, AnswerValue::Selected("Y".to_string()));
        assert_eq!(a.text, "Yes, deploy");
        assert!(a.selected_option.is_some());
    }

    #[test]
    fn answer_serde_roundtrip() {
        let a = Answer::yes();
        let json = serde_json::to_string(&a).unwrap();
        let back: Answer = serde_json::from_str(&json).unwrap();
        assert_eq!(back.value, AnswerValue::Yes);
    }

    // -- AutoApproveInterviewer --

    #[tokio::test]
    async fn auto_approve_yesno() {
        let iv = AutoApproveInterviewer;
        let a = iv.ask(make_yesno_question()).await;
        assert_eq!(a.value, AnswerValue::Yes);
    }

    #[tokio::test]
    async fn auto_approve_confirmation() {
        let iv = AutoApproveInterviewer;
        let q = Question {
            question_type: QuestionType::Confirmation,
            ..make_yesno_question()
        };
        let a = iv.ask(q).await;
        assert_eq!(a.value, AnswerValue::Yes);
    }

    #[tokio::test]
    async fn auto_approve_multiple_choice_returns_first() {
        let iv = AutoApproveInterviewer;
        let q = make_mc_question(vec![("A", "Approve"), ("R", "Reject")]);
        let a = iv.ask(q).await;
        assert_eq!(a.value, AnswerValue::Selected("A".to_string()));
        assert_eq!(a.text, "Approve");
    }

    #[tokio::test]
    async fn auto_approve_empty_mc() {
        let iv = AutoApproveInterviewer;
        let q = make_mc_question(vec![]);
        let a = iv.ask(q).await;
        assert_eq!(a.value, AnswerValue::Selected("auto-approved".to_string()));
    }

    #[tokio::test]
    async fn auto_approve_freeform() {
        let iv = AutoApproveInterviewer;
        let q = Question {
            question_type: QuestionType::Freeform,
            ..make_yesno_question()
        };
        let a = iv.ask(q).await;
        assert_eq!(a.value, AnswerValue::Selected("auto-approved".to_string()));
    }

    #[tokio::test]
    async fn auto_approve_ask_multiple() {
        let iv = AutoApproveInterviewer;
        let questions = vec![make_yesno_question(), make_yesno_question()];
        let answers = iv.ask_multiple(questions).await;
        assert_eq!(answers.len(), 2);
        assert!(answers.iter().all(|a| a.value == AnswerValue::Yes));
    }

    // -- QueueInterviewer --

    #[tokio::test]
    async fn queue_pops_front() {
        let iv = QueueInterviewer::new(vec![Answer::yes(), Answer::no()]);
        assert_eq!(iv.remaining(), 2);
        let a1 = iv.ask(make_yesno_question()).await;
        assert_eq!(a1.value, AnswerValue::Yes);
        assert_eq!(iv.remaining(), 1);
        let a2 = iv.ask(make_yesno_question()).await;
        assert_eq!(a2.value, AnswerValue::No);
        assert_eq!(iv.remaining(), 0);
    }

    #[tokio::test]
    async fn queue_empty_returns_skipped() {
        let iv = QueueInterviewer::new(vec![]);
        let a = iv.ask(make_yesno_question()).await;
        assert_eq!(a.value, AnswerValue::Skipped);
    }

    #[tokio::test]
    async fn queue_push_appends() {
        let iv = QueueInterviewer::new(vec![Answer::yes()]);
        iv.push(Answer::no());
        assert_eq!(iv.remaining(), 2);
        iv.ask(make_yesno_question()).await; // yes
        let a = iv.ask(make_yesno_question()).await; // no
        assert_eq!(a.value, AnswerValue::No);
    }

    #[tokio::test]
    async fn queue_ask_multiple_count() {
        let iv = QueueInterviewer::new(vec![Answer::yes(), Answer::yes(), Answer::yes()]);
        let answers = iv
            .ask_multiple(vec![
                make_yesno_question(),
                make_yesno_question(),
                make_yesno_question(),
            ])
            .await;
        assert_eq!(answers.len(), 3);
    }
}
