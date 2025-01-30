import AltText from '../Components/Forms/AltText'
import AnchorText from '../Components/Forms/AnchorText'
import ContrastForm from '../Components/Forms/ContrastForm'
import UfixitReviewOnly from '../Components/Forms/UfixitReviewOnly'
import HeadingEmptyForm from '../Components/Forms/HeadingEmptyForm'
import HeadingStyleForm from '../Components/Forms/HeadingStyleForm'
import TableHeaders from '../Components/Forms/TableHeaders'
import Video from '../Components/Forms/Video'
import LinkForm from '../Components/Forms/LinkForm'
import EmphasisForm from '../Components/Forms/EmphasisForm'
import LabelForm from '../Components/Forms/LabelForm'
import QuoteForm from '../Components/Forms/QuoteForm'
import StyleMisuseForm from '../Components/Forms/StyleMisuseForm'
import SensoryMisuseForm from '../Components/Forms/SensoryMisuseForm'

const UfixitForms = {
  // phpAlly rules
  AnchorMustContainText: AnchorText,
  AnchorSuspiciousLinkText: AnchorText,
  BrokenLink: LinkForm,
  CssTextHasContrast: ContrastForm,
  CssTextStyleEmphasize: EmphasisForm,
  HeadersHaveText: HeadingEmptyForm,
  ImageAltIsDifferent: AltText,
  ImageAltIsTooLong: AltText,
  ImageAltNotEmptyInAnchor: AltText,
  ImageHasAlt: AltText,
  ImageHasAltDecorative: AltText,
  ParagraphNotUsedAsHeader: HeadingStyleForm,
  RedirectedLink: LinkForm,
  TableDataShouldHaveTableHeader: TableHeaders,
  TableHeaderShouldHaveScope: TableHeaders,
  ImageAltNotPlaceholder: AltText,
  VideoCaptionsMatchCourseLanguage: Video,
  VideosEmbeddedOrLinkedNeedCaptions: Video,
  VideosHaveAutoGeneratedCaptions: Video,

  // Equal Access Rules
  img_alt_misuse: AltText,
  aria_application_labelled: LabelForm,
  aria_application_label_unique: LabelForm,
  text_contrast_sufficient: ContrastForm,
  text_block_heading: HeadingStyleForm,
  heading_content_exists: HeadingEmptyForm,
  // text_quoted_correctly: QuoteForm,
  style_color_misuse: StyleMisuseForm,
  text_sensory_misuse: SensoryMisuseForm,
}

export function returnIssueForm(activeIssue) {
  if (activeIssue) {
    if (UfixitForms.hasOwnProperty(activeIssue.scanRuleId)) {
      if (!activeIssue.sourceHtml.includes("data-type-module")) {
        return UfixitForms[activeIssue.scanRuleId]
      }
    }
  }

  return UfixitReviewOnly
}
