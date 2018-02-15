import {Selector} from 'testcafe'
import {pageURL} from '../../helpers'
import {removeData, insertTestUser} from '../../envtools';
import {signIn} from '../../roles';
import {PageConfigurationBasic} from '../../page-models/PageConfigurationBasic'
import {successNotification} from '../../components/notifications'

fixture('Basic configuration')
    .beforeEach(async(t) => {
        await removeData();
        await insertTestUser();
        await signIn(t);
        await t.navigateTo(pageURL(`configuration/new/basic`))
    })
    .after(removeData)

test('Off-heap size visibility for different Ignite versions', async (t) => {
    const page = new PageConfigurationBasic
    const ignite2 = 'Ignite 2.4'
    const ignite1 = 'Ignite 1.x'

    await page.versionPicker.pickVersion(ignite2)
    await t.expect(page.totalOffheapSizeInput.exists).ok('Visible in latest 2.x version')
    await page.versionPicker.pickVersion(ignite1)
    await t.expect(page.totalOffheapSizeInput.count).eql(0, 'Invisible in Ignite 1.x')
})

test('Default form action', async (t) => {
    const page = new PageConfigurationBasic

    await t.expect(page.mainFormAction.textContent).eql(PageConfigurationBasic.SAVE_CHANGES_AND_DOWNLOAD_LABEL)
})

test('Basic editing', async (t) => {
    const page = new PageConfigurationBasic
    const clusterName = 'Test basic cluster #1'
    const localMode = 'LOCAL'
    const atomic = 'ATOMIC'

    await t
        .expect(page.buttonPreviewProject.visible).notOk('Preview project button is hidden for new cluster configs')
        .expect(page.buttonDownloadProject.visible).notOk('Download project button is hidden for new cluster configs')
        .typeText(page.clusterNameInput.control, clusterName, {replace: true})
    await page.cachesList.addItem()
    await page.cachesList.addItem()
    await page.cachesList.addItem()

    const cache1 = page.cachesList.getItem(1)
    await cache1.startEdit()
    await t.typeText(cache1.fields.name.control, 'Foobar')
    await cache1.fields.cacheMode.selectOption(localMode)
    await cache1.fields.atomicityMode.selectOption(atomic)
    await cache1.stopEdit()

    await t.expect(cache1.getItemViewColumn(0).textContent).contains(`Cache1Foobar`, 'Can edit cache name')
    await t.expect(cache1.getItemViewColumn(1).textContent).eql(localMode, 'Can edit cache mode')
    await t.expect(cache1.getItemViewColumn(2).textContent).eql(atomic, 'Can edit cache atomicity')

    await page.save()
    await t
        .expect(successNotification.visible).ok('Shows success notifications')
        .expect(successNotification.textContent).contains(`Cluster "${clusterName}" saved.`, 'Success notification has correct text', {timeout: 500})
    await t.eval(() => window.location.reload())
    await t.expect(page.pageHeader.textContent).contains(`Edit cluster configuration ‘${clusterName}’`)
})